-- Additive, versioned NAV evidence. Apply only to an explicitly selected schema.
-- PR132's nav_timeseries provenance upgrade is a prerequisite, not repeated here.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS nav_policy_versions (
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_hash char(64) NOT NULL,
    readiness_profile text NOT NULL CHECK (readiness_profile = 'current_daily_nav_v1'),
    valuation_frequency text NOT NULL CHECK (valuation_frequency = 'daily'),
    calendar_id varchar(128) NOT NULL,
    calendar_version varchar(64) NOT NULL,
    calendar_source text NOT NULL CHECK (length(btrim(calendar_source)) > 0),
    timezone text NOT NULL CHECK (timezone = 'America/New_York'),
    coverage_start date NOT NULL,
    coverage_end date NOT NULL,
    valid_through timestamptz NOT NULL,
    calendar_session_count integer NOT NULL CHECK (calendar_session_count >= 401),
    calendar_digest char(64) NOT NULL,
    sample_intervals integer NOT NULL CHECK (sample_intervals = 400),
    annualization_sessions integer NOT NULL CHECK (annualization_sessions = 252),
    required_nav_kind text NOT NULL CHECK (required_nav_kind = 'adjusted'),
    required_return_semantics text NOT NULL CHECK (required_return_semantics = 'observed_interval_log_ratio'),
    modeling_currency varchar(3) NOT NULL CHECK (modeling_currency = 'USD'),
    currency_treatment text NOT NULL CHECK (currency_treatment = 'native_only'),
    source_reference text NOT NULL CHECK (length(btrim(source_reference)) > 0),
    published_at timestamptz,
    CHECK (coverage_start < coverage_end),
    PRIMARY KEY (policy_id, policy_version)
);

-- Each due instant is established from the actual valuation publication schedule.
-- No weekday generator, holiday inference or guessed deadline is permitted.
CREATE TABLE IF NOT EXISTS nav_valuation_schedules (
    calendar_id varchar(128) NOT NULL,
    calendar_version varchar(64) NOT NULL,
    session_date date NOT NULL,
    valuation_close_at timestamptz NOT NULL,
    nav_due_at timestamptz NOT NULL CHECK (nav_due_at >= valuation_close_at),
    calendar_source text NOT NULL CHECK (length(btrim(calendar_source)) > 0),
    source_reference text NOT NULL CHECK (length(btrim(source_reference)) > 0),
    PRIMARY KEY (calendar_id, calendar_version, session_date)
);
CREATE INDEX IF NOT EXISTS nav_valuation_schedules_due_idx
    ON nav_valuation_schedules (calendar_id, calendar_version, nav_due_at DESC);

CREATE TABLE IF NOT EXISTS nav_policy_current (
    readiness_profile text PRIMARY KEY CHECK (readiness_profile = 'current_daily_nav_v1'),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions
);

CREATE OR REPLACE FUNCTION nav_policy_freeze_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    calendar_key varchar(128);
    version_key varchar(64);
BEGIN
    IF TG_TABLE_NAME = 'nav_policy_versions' THEN
        IF TG_OP = 'DELETE' OR OLD.published_at IS NOT NULL THEN
            RAISE EXCEPTION 'published NAV policy is immutable';
        END IF;
        IF NEW.policy_id IS DISTINCT FROM OLD.policy_id
           OR NEW.policy_version IS DISTINCT FROM OLD.policy_version
           OR NEW.policy_hash IS DISTINCT FROM OLD.policy_hash
           OR NEW.readiness_profile IS DISTINCT FROM OLD.readiness_profile
           OR NEW.timezone IS DISTINCT FROM OLD.timezone
           OR NEW.coverage_start IS DISTINCT FROM OLD.coverage_start
           OR NEW.coverage_end IS DISTINCT FROM OLD.coverage_end
           OR NEW.valid_through IS DISTINCT FROM OLD.valid_through
           OR NEW.calendar_session_count IS DISTINCT FROM OLD.calendar_session_count
           OR NEW.calendar_digest IS DISTINCT FROM OLD.calendar_digest
           OR NEW.sample_intervals IS DISTINCT FROM OLD.sample_intervals
           OR NEW.annualization_sessions IS DISTINCT FROM OLD.annualization_sessions
           OR NEW.required_nav_kind IS DISTINCT FROM OLD.required_nav_kind
           OR NEW.required_return_semantics IS DISTINCT FROM OLD.required_return_semantics
           OR NEW.modeling_currency IS DISTINCT FROM OLD.modeling_currency
           OR NEW.currency_treatment IS DISTINCT FROM OLD.currency_treatment
           OR NEW.calendar_id IS DISTINCT FROM OLD.calendar_id
           OR NEW.calendar_version IS DISTINCT FROM OLD.calendar_version
           OR NEW.calendar_source IS DISTINCT FROM OLD.calendar_source
           OR NEW.valuation_frequency IS DISTINCT FROM OLD.valuation_frequency
           OR NEW.source_reference IS DISTINCT FROM OLD.source_reference THEN
            RAISE EXCEPTION 'NAV policy content cannot change during publication';
        END IF;
    ELSIF TG_TABLE_NAME = 'nav_valuation_schedules' THEN
        IF TG_OP = 'INSERT' THEN
            calendar_key := NEW.calendar_id;
            version_key := NEW.calendar_version;
        ELSE
            calendar_key := OLD.calendar_id;
            version_key := OLD.calendar_version;
        END IF;
        IF EXISTS (
            SELECT 1 FROM nav_policy_versions p
            WHERE p.calendar_id = calendar_key
              AND p.calendar_version = version_key
              AND p.published_at IS NOT NULL
        ) THEN
            RAISE EXCEPTION 'published valuation calendar is immutable';
        END IF;
        IF TG_OP = 'UPDATE' AND (NEW.calendar_id IS DISTINCT FROM OLD.calendar_id
            OR NEW.calendar_version IS DISTINCT FROM OLD.calendar_version) THEN
            RAISE EXCEPTION 'valuation calendar identity is immutable';
        END IF;
    ELSE
        RAISE EXCEPTION 'unsupported NAV policy evidence mutation';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END$$;
DROP TRIGGER IF EXISTS nav_policy_freeze ON nav_policy_versions;
CREATE TRIGGER nav_policy_freeze BEFORE UPDATE OR DELETE ON nav_policy_versions
FOR EACH ROW EXECUTE FUNCTION nav_policy_freeze_v1();
DROP TRIGGER IF EXISTS nav_schedule_freeze ON nav_valuation_schedules;
CREATE TRIGGER nav_schedule_freeze BEFORE INSERT OR UPDATE OR DELETE ON nav_valuation_schedules
FOR EACH ROW EXECUTE FUNCTION nav_policy_freeze_v1();

-- Lifecycle/identity evidence is temporal and distinct from catalogue discovery.
-- Absence of a row means UNKNOWN, even when instruments_universe.is_active=true.
CREATE TABLE IF NOT EXISTS nav_instrument_policy_evidence (
    evidence_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    instrument_id uuid NOT NULL,
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    known_at timestamptz NOT NULL,
    effective_at timestamptz NOT NULL,
    fund_status text NOT NULL CHECK (fund_status IN ('ACTIVE','INACTIVE','UNKNOWN')),
    valuation_frequency text NOT NULL CHECK (valuation_frequency IN ('daily','weekly','monthly','unknown')),
    identity_verified boolean NOT NULL DEFAULT false,
    return_basis_verified boolean NOT NULL DEFAULT false,
    currency_verified boolean NOT NULL DEFAULT false,
    evidence_reference text NOT NULL CHECK (length(btrim(evidence_reference)) > 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (instrument_id, policy_id, policy_version, known_at, effective_at),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions
);
CREATE INDEX IF NOT EXISTS nav_instrument_policy_latest_idx
    ON nav_instrument_policy_evidence (instrument_id, policy_id, policy_version, known_at DESC);
CREATE OR REPLACE FUNCTION nav_instrument_evidence_append_only_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'NAV lifecycle evidence is append-only';
END$$;
DROP TRIGGER IF EXISTS nav_instrument_evidence_append_only ON nav_instrument_policy_evidence;
CREATE TRIGGER nav_instrument_evidence_append_only
BEFORE UPDATE OR DELETE ON nav_instrument_policy_evidence
FOR EACH ROW EXECUTE FUNCTION nav_instrument_evidence_append_only_v1();

CREATE TABLE IF NOT EXISTS nav_ingestion_runs (
    run_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    requested_end date NOT NULL,
    status text NOT NULL CHECK (status IN ('running','completed','aborted'))
);
CREATE TABLE IF NOT EXISTS nav_ingestion_attempts (
    run_id uuid NOT NULL REFERENCES nav_ingestion_runs,
    instrument_id uuid NOT NULL,
    ticker text NOT NULL,
    provider text NOT NULL,
    requested_start date NOT NULL,
    requested_end date NOT NULL,
    attempted_at timestamptz,
    finished_at timestamptz,
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    status text NOT NULL CHECK (status IN (
        'success_new','success_no_new','empty','not_found','invalid_payload',
        'rate_limited','transient_error','not_configured','not_attempted_budget',
        'not_due')),
    newest_observed_date date,
    row_count integer NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    reason_code varchar(48),
    PRIMARY KEY (run_id, instrument_id, provider),
    CHECK (finished_at IS NULL OR attempted_at IS NULL OR finished_at >= attempted_at),
    CHECK (reason_code IS NULL OR reason_code ~ '^[A-Z_]{1,48}$')
);
CREATE INDEX IF NOT EXISTS nav_ingestion_attempts_recent_idx
    ON nav_ingestion_attempts (instrument_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS nav_ingestion_attempts_persisted_idx
    ON nav_ingestion_attempts (instrument_id, persisted_at DESC);

-- Calendar maintenance is not ingestion: it never fetches, never creates a
-- provider run/attempt and may only stamp the pinned published calendar tuple
-- on already-typed rows. Scope, pins and completion are validated in the DB.
CREATE OR REPLACE FUNCTION nav_uuid_array_unique_v1(ids uuid[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$
SELECT count(*) = count(DISTINCT id) AND count(id) = count(*) FROM unnest(ids) AS id
$$;
CREATE TABLE IF NOT EXISTS nav_calendar_maintenance_runs (
    maintenance_run_id uuid PRIMARY KEY,
    operation text NOT NULL CHECK (operation = 'calendar_stamp'),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_hash char(64) NOT NULL,
    calendar_id varchar(128) NOT NULL,
    calendar_version varchar(64) NOT NULL,
    calendar_source text NOT NULL,
    plan_sha256 char(64) NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    instrument_ids uuid[] NOT NULL CHECK (
        array_ndims(instrument_ids) = 1
        AND cardinality(instrument_ids) BETWEEN 1 AND 20
        AND nav_uuid_array_unique_v1(instrument_ids)),
    window_start date NOT NULL,
    window_end date NOT NULL,
    status text NOT NULL CHECK (status IN ('running','completed')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    changed_rows integer CHECK (changed_rows >= 0),
    before_digest char(64) NOT NULL CHECK (before_digest ~ '^[0-9a-f]{64}$'),
    after_digest char(64) CHECK (after_digest ~ '^[0-9a-f]{64}$'),
    CHECK (window_start <= window_end AND window_end - window_start <= 600),
    CHECK ((status = 'running' AND completed_at IS NULL AND changed_rows IS NULL
            AND after_digest IS NULL)
        OR (status = 'completed' AND completed_at IS NOT NULL
            AND changed_rows IS NOT NULL AND after_digest IS NOT NULL)),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions
);

-- A row revision is recorded in the *same* transaction as any NAV/return change,
-- regardless of writer (ingestion, operator or reprocessor). The head row lock
-- serializes revision IDs for one instrument even under concurrent transactions.
-- Provider and maintenance attribution are exclusive; both NULL is an
-- unattributed write and never admissible evidence.
CREATE TABLE IF NOT EXISTS fund_nav_data_heads (
    instrument_id uuid PRIMARY KEY,
    revision_id bigint NOT NULL DEFAULT 0 CHECK (revision_id >= 0)
);
CREATE TABLE IF NOT EXISTS fund_nav_data_revisions (
    revision_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_id uuid NOT NULL,
    nav_date date NOT NULL,
    mutation_kind text NOT NULL CHECK (mutation_kind IN ('INSERT','UPDATE','DELETE')),
    source_run_id uuid REFERENCES nav_ingestion_runs(run_id),
    maintenance_run_id uuid REFERENCES nav_calendar_maintenance_runs(maintenance_run_id),
    data_changed boolean NOT NULL,
    calendar_changed boolean NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (source_run_id IS NULL OR maintenance_run_id IS NULL),
    CHECK (data_changed OR calendar_changed),
    CHECK (mutation_kind = 'UPDATE' OR data_changed),
    CHECK (maintenance_run_id IS NULL
        OR (mutation_kind = 'UPDATE' AND NOT data_changed AND calendar_changed))
);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_instrument_idx
    ON fund_nav_data_revisions (instrument_id, revision_id DESC);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_window_idx
    ON fund_nav_data_revisions (instrument_id, nav_date, revision_id DESC);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_maintenance_idx
    ON fund_nav_data_revisions (maintenance_run_id)
    WHERE maintenance_run_id IS NOT NULL;

CREATE OR REPLACE FUNCTION nav_calendar_maintenance_guard_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    revision_count bigint;
    revisions_in_scope boolean;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'running' THEN
            RAISE EXCEPTION 'NAV maintenance run must be registered running';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM nav_policy_current c
            JOIN nav_policy_versions p USING (policy_id, policy_version)
            WHERE c.readiness_profile = 'current_daily_nav_v1'
              AND p.policy_id = NEW.policy_id AND p.policy_version = NEW.policy_version
              AND p.policy_hash = NEW.policy_hash AND p.published_at IS NOT NULL
              AND p.valid_through >= clock_timestamp()
              AND p.calendar_id = NEW.calendar_id
              AND p.calendar_version = NEW.calendar_version
              AND p.calendar_source = NEW.calendar_source
        ) THEN
            RAISE EXCEPTION 'NAV maintenance must pin the current published policy calendar';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' OR OLD.status <> 'running' THEN
        RAISE EXCEPTION 'NAV maintenance run is immutable';
    END IF;
    IF NEW.status <> 'completed'
       OR (NEW.maintenance_run_id, NEW.operation, NEW.policy_id, NEW.policy_version,
           NEW.policy_hash, NEW.calendar_id, NEW.calendar_version, NEW.calendar_source,
           NEW.plan_sha256, NEW.instrument_ids, NEW.window_start, NEW.window_end,
           NEW.started_at, NEW.before_digest)
          IS DISTINCT FROM
          (OLD.maintenance_run_id, OLD.operation, OLD.policy_id, OLD.policy_version,
           OLD.policy_hash, OLD.calendar_id, OLD.calendar_version, OLD.calendar_source,
           OLD.plan_sha256, OLD.instrument_ids, OLD.window_start, OLD.window_end,
           OLD.started_at, OLD.before_digest) THEN
        RAISE EXCEPTION 'NAV maintenance run may only transition running to completed';
    END IF;
    SELECT count(*), COALESCE(bool_and(
               r.mutation_kind = 'UPDATE' AND NOT r.data_changed AND r.calendar_changed
               AND r.source_run_id IS NULL
               AND r.instrument_id = ANY(OLD.instrument_ids)
               AND r.nav_date BETWEEN OLD.window_start AND OLD.window_end), true)
      INTO revision_count, revisions_in_scope
    FROM fund_nav_data_revisions r
    WHERE r.maintenance_run_id = OLD.maintenance_run_id;
    IF revision_count <> NEW.changed_rows OR NOT revisions_in_scope THEN
        RAISE EXCEPTION 'NAV maintenance completion does not match its attributed revisions';
    END IF;
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_calendar_maintenance_guard ON nav_calendar_maintenance_runs;
CREATE TRIGGER nav_calendar_maintenance_guard
BEFORE INSERT OR UPDATE OR DELETE ON nav_calendar_maintenance_runs
FOR EACH ROW EXECUTE FUNCTION nav_calendar_maintenance_guard_v1();
-- Revisions are append-only and only the nav_timeseries stamp trigger may write
-- them (nested trigger depth), so lineage cannot be forged by a direct INSERT.
CREATE OR REPLACE FUNCTION fund_nav_revision_append_only_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV data revisions are append-only';
    END IF;
    IF pg_trigger_depth() < 2 THEN
        RAISE EXCEPTION 'NAV data revisions are recorded only by the NAV stamp trigger';
    END IF;
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS fund_nav_revision_append_only ON fund_nav_data_revisions;
CREATE TRIGGER fund_nav_revision_append_only
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_data_revisions
FOR EACH ROW EXECUTE FUNCTION fund_nav_revision_append_only_v1();
CREATE TABLE IF NOT EXISTS fund_nav_reexpression_holds (
    instrument_id uuid PRIMARY KEY,
    first_changed_date date NOT NULL,
    last_changed_date date NOT NULL,
    source_run_id uuid,
    reason_code text NOT NULL CHECK (reason_code = 'ADJUSTED_HISTORY_REEXPRESSION'),
    detected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (first_changed_date <= last_changed_date)
);
-- The calendar tuple (calendar_id, calendar_version, calendar_source) is one
-- assertion: partial tuples and resets to NULL are rejected, and a new stamp
-- must be an attributed session of the current published policy. UPDATE
-- revisions distinguish economic data from calendar-only metadata.
CREATE OR REPLACE FUNCTION fund_nav_stamp_revision_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    affected_instrument uuid;
    affected_date date;
    new_revision bigint;
    ingestion_run uuid;
    maintenance_run uuid;
    data_change boolean;
    calendar_change boolean;
    asserts_stamp boolean := false;
    maintenance record;
    lifecycle record;
BEGIN
    ingestion_run := NULLIF(current_setting('nav.ingestion_run_id', true), '')::uuid;
    maintenance_run := NULLIF(current_setting('nav.maintenance_run_id', true), '')::uuid;
    IF ingestion_run IS NOT NULL AND maintenance_run IS NOT NULL THEN
        RAISE EXCEPTION 'NAV ingestion and maintenance contexts are mutually exclusive';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.instrument_id IS DISTINCT FROM NEW.instrument_id
           OR OLD.nav_date IS DISTINCT FROM NEW.nav_date THEN
            RAISE EXCEPTION 'NAV instrument/date identity cannot be moved';
        END IF;
        IF to_jsonb(OLD) = to_jsonb(NEW) THEN
            RETURN NEW;
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        affected_instrument := OLD.instrument_id;
        affected_date := OLD.nav_date;
        data_change := true;
        calendar_change := num_nulls(OLD.calendar_id, OLD.calendar_version,
                                     OLD.calendar_source) < 3;
    ELSE
        affected_instrument := NEW.instrument_id;
        affected_date := NEW.nav_date;
        IF num_nulls(NEW.calendar_id, NEW.calendar_version, NEW.calendar_source)
           NOT IN (0, 3) THEN
            RAISE EXCEPTION 'NAV calendar tuple is indivisible';
        END IF;
        IF TG_OP = 'INSERT' THEN
            data_change := true;
            calendar_change := NEW.calendar_id IS NOT NULL;
        ELSE
            IF num_nulls(OLD.calendar_id, OLD.calendar_version, OLD.calendar_source) < 3
               AND NEW.calendar_id IS NULL THEN
                RAISE EXCEPTION 'NAV calendar tuple cannot be reset to NULL';
            END IF;
            calendar_change := (OLD.calendar_id, OLD.calendar_version, OLD.calendar_source)
                IS DISTINCT FROM (NEW.calendar_id, NEW.calendar_version, NEW.calendar_source);
            data_change :=
                (to_jsonb(OLD) - ARRAY['calendar_id','calendar_version','calendar_source'])
                IS DISTINCT FROM
                (to_jsonb(NEW) - ARRAY['calendar_id','calendar_version','calendar_source']);
        END IF;
        asserts_stamp := calendar_change;
    END IF;
    IF asserts_stamp THEN
        IF ingestion_run IS NULL AND maintenance_run IS NULL THEN
            RAISE EXCEPTION 'NAV calendar stamp requires provider or maintenance attribution';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM nav_policy_current c
            JOIN nav_policy_versions p USING (policy_id, policy_version)
            JOIN nav_valuation_schedules s
              ON s.calendar_id = p.calendar_id AND s.calendar_version = p.calendar_version
             AND s.session_date = NEW.nav_date AND s.calendar_source = p.calendar_source
            WHERE c.readiness_profile = 'current_daily_nav_v1'
              AND p.published_at IS NOT NULL AND p.valid_through >= clock_timestamp()
              AND p.calendar_id = NEW.calendar_id
              AND p.calendar_version = NEW.calendar_version
              AND p.calendar_source = NEW.calendar_source
        ) THEN
            RAISE EXCEPTION 'NAV calendar stamp must be a session of the current published policy';
        END IF;
    END IF;
    IF maintenance_run IS NOT NULL THEN
        IF TG_OP <> 'UPDATE' THEN
            RAISE EXCEPTION 'NAV maintenance may only update calendar metadata';
        END IF;
        IF data_change OR NOT calendar_change
           OR num_nulls(OLD.calendar_id, OLD.calendar_version, OLD.calendar_source) <> 3 THEN
            RAISE EXCEPTION 'NAV maintenance may only stamp calendar metadata on unstamped rows';
        END IF;
        SELECT * INTO maintenance FROM nav_calendar_maintenance_runs
        WHERE maintenance_run_id = maintenance_run FOR SHARE;
        IF NOT FOUND OR maintenance.status <> 'running' THEN
            RAISE EXCEPTION 'NAV maintenance run is not running';
        END IF;
        IF NOT (NEW.instrument_id = ANY(maintenance.instrument_ids))
           OR NEW.nav_date NOT BETWEEN maintenance.window_start AND maintenance.window_end
           OR (NEW.calendar_id, NEW.calendar_version, NEW.calendar_source)
              IS DISTINCT FROM (maintenance.calendar_id, maintenance.calendar_version,
                                maintenance.calendar_source) THEN
            RAISE EXCEPTION 'NAV maintenance write is outside its pinned scope';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM nav_policy_current c
            JOIN nav_policy_versions p USING (policy_id, policy_version)
            WHERE c.readiness_profile = 'current_daily_nav_v1'
              AND p.policy_id = maintenance.policy_id
              AND p.policy_version = maintenance.policy_version
              AND p.policy_hash = maintenance.policy_hash
        ) THEN
            RAISE EXCEPTION 'NAV maintenance policy pin is no longer current';
        END IF;
        IF NOT COALESCE(NEW.source_nav IS NOT NULL AND NEW.source_nav_kind = 'adjusted'
                        AND NEW.currency = 'USD' AND NEW.nav_repair_kind = 'none'
                        AND NEW.source_nav = NEW.nav, false) THEN
            RAISE EXCEPTION 'NAV maintenance row fails the economic stamp contract';
        END IF;
        SELECT e.valuation_frequency, e.identity_verified, e.return_basis_verified,
               e.currency_verified
          INTO lifecycle
        FROM nav_instrument_policy_evidence e
        WHERE e.instrument_id = NEW.instrument_id
          AND e.policy_id = maintenance.policy_id
          AND e.policy_version = maintenance.policy_version
          AND e.known_at <= clock_timestamp() AND e.effective_at <= clock_timestamp()
        ORDER BY e.effective_at DESC, e.known_at DESC LIMIT 1;
        IF NOT FOUND OR NOT COALESCE(lifecycle.valuation_frequency = 'daily'
                                     AND lifecycle.identity_verified
                                     AND lifecycle.return_basis_verified
                                     AND lifecycle.currency_verified, false) THEN
            RAISE EXCEPTION 'NAV maintenance requires current verified daily lifecycle evidence';
        END IF;
    END IF;
    INSERT INTO fund_nav_data_heads (instrument_id) VALUES (affected_instrument)
    ON CONFLICT (instrument_id) DO NOTHING;
    PERFORM 1 FROM fund_nav_data_heads WHERE instrument_id = affected_instrument FOR UPDATE;
    INSERT INTO fund_nav_data_revisions
        (instrument_id, nav_date, mutation_kind, source_run_id, maintenance_run_id,
         data_changed, calendar_changed)
    VALUES (affected_instrument, affected_date, TG_OP, ingestion_run, maintenance_run,
            data_change, calendar_change)
    RETURNING revision_id INTO new_revision;
    UPDATE fund_nav_data_heads SET revision_id = new_revision
    WHERE instrument_id = affected_instrument;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END$$;
DROP TRIGGER IF EXISTS fund_nav_stamp_revision ON nav_timeseries;
CREATE TRIGGER fund_nav_stamp_revision
AFTER INSERT OR UPDATE OR DELETE ON nav_timeseries
FOR EACH ROW EXECUTE FUNCTION fund_nav_stamp_revision_v1();

-- Inserted in the SAME transaction as the risk metric values; the historical
-- risk/MV contracts are not rewritten or retroactively called verified.
-- Each run pins its scope, policy/due session, requested inputs, target set and
-- one feature definition before any evidence is written. Only current_full runs
-- may be promoted by the publisher; diagnostic runs carry a closed reason.
CREATE TABLE IF NOT EXISTS fund_nav_risk_runs (
    risk_run_id uuid PRIMARY KEY,
    calc_date date NOT NULL,
    run_scope text NOT NULL CHECK (run_scope IN ('current_full','diagnostic')),
    nonpublishing_reason text CHECK (nonpublishing_reason IN (
        'LIMITED_RUN','NON_CURRENT_SESSION','POLICY_UNAVAILABLE','POLICY_EXPIRED')),
    policy_id text,
    policy_version text,
    policy_hash char(64),
    due_session date,
    requested_calc_date date,
    requested_limit integer CHECK (requested_limit >= 0),
    universe_digest char(64) NOT NULL CHECK (universe_digest ~ '^[0-9a-f]{64}$'),
    feature_definition_version text NOT NULL
        CHECK (length(btrim(feature_definition_version)) > 0),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN ('running','metrics_complete','complete')),
    expected_rows integer NOT NULL CHECK (expected_rows >= 0),
    persisted_rows integer CHECK (persisted_rows >= 0),
    CHECK (num_nulls(policy_id, policy_version, policy_hash) IN (0, 3)),
    CHECK ((run_scope = 'current_full' AND nonpublishing_reason IS NULL
            AND policy_id IS NOT NULL AND due_session IS NOT NULL
            AND calc_date = due_session AND requested_limit IS NULL)
        OR (run_scope = 'diagnostic' AND nonpublishing_reason IS NOT NULL)),
    CHECK ((status = 'running' AND completed_at IS NULL AND persisted_rows IS NULL)
        OR (status = 'metrics_complete' AND completed_at IS NULL
            AND persisted_rows = expected_rows)
        OR (status = 'complete' AND completed_at IS NOT NULL
            AND persisted_rows = expected_rows)),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions
);
-- The exact target set is registered with the run and frozen before evidence.
CREATE TABLE IF NOT EXISTS fund_nav_risk_run_members (
    risk_run_id uuid NOT NULL REFERENCES fund_nav_risk_runs,
    instrument_id uuid NOT NULL,
    PRIMARY KEY (risk_run_id, instrument_id)
);
CREATE TABLE IF NOT EXISTS fund_nav_risk_publication (
    readiness_profile text PRIMARY KEY CHECK (readiness_profile = 'current_daily_nav_v1'),
    revision_id bigint NOT NULL DEFAULT 0 CHECK (revision_id >= 0),
    state text NOT NULL CHECK (state IN ('running','idle')),
    active_risk_run_id uuid REFERENCES fund_nav_risk_runs,
    published_risk_run_id uuid REFERENCES fund_nav_risk_runs,
    CHECK ((state='running' AND active_risk_run_id IS NOT NULL)
        OR (state='idle' AND active_risk_run_id IS NULL))
);
-- Features and exclusions are append-only facts of one (run, member). Their
-- union must equal the member set and their intersection must be empty before
-- the run can leave `running`; duplicates are errors, never last-write-wins.
CREATE TABLE IF NOT EXISTS fund_nav_risk_exclusions (
    risk_run_id uuid NOT NULL,
    instrument_id uuid NOT NULL,
    calc_date date NOT NULL,
    reason_code text NOT NULL CHECK (reason_code IN ('NAV_WINDOW_TOO_SHORT','METRICS_UNAVAILABLE')),
    nav_count integer NOT NULL CHECK (nav_count >= 0),
    input_max_date date,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (risk_run_id, instrument_id),
    FOREIGN KEY (risk_run_id, instrument_id) REFERENCES fund_nav_risk_run_members
);
CREATE INDEX IF NOT EXISTS fund_nav_risk_exclusions_recent_idx
    ON fund_nav_risk_exclusions (instrument_id, calc_date DESC);
CREATE TABLE IF NOT EXISTS fund_nav_feature_evidence (
    instrument_id uuid NOT NULL,
    calc_date date NOT NULL,
    definition_version text NOT NULL,
    risk_run_id uuid NOT NULL,
    feature_as_of date NOT NULL,
    input_max_date date NOT NULL,
    nav_start date NOT NULL,
    nav_end date NOT NULL,
    nav_count integer NOT NULL CHECK (nav_count >= 22),
    input_fingerprint char(64) NOT NULL,
    nav_input_fingerprint char(64) NOT NULL,
    benchmark_evidence jsonb NOT NULL,
    factor_evidence jsonb NOT NULL,
    exclusion_reason text,
    computed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (risk_run_id, instrument_id),
    FOREIGN KEY (risk_run_id, instrument_id) REFERENCES fund_nav_risk_run_members,
    CHECK (nav_start <= nav_end AND nav_end = input_max_date)
);
CREATE INDEX IF NOT EXISTS fund_nav_feature_evidence_identity_idx
    ON fund_nav_feature_evidence (instrument_id, calc_date DESC, definition_version, risk_run_id);

CREATE OR REPLACE FUNCTION fund_nav_risk_run_guard_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    member_count bigint;
    uncovered bigint;
    overlapping bigint;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'running' OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION 'NAV risk run must be registered running';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'NAV risk runs are append-only';
    END IF;
    IF (NEW.risk_run_id, NEW.calc_date, NEW.run_scope, NEW.nonpublishing_reason,
        NEW.policy_id, NEW.policy_version, NEW.policy_hash, NEW.due_session,
        NEW.requested_calc_date, NEW.requested_limit, NEW.universe_digest,
        NEW.feature_definition_version, NEW.started_at, NEW.expected_rows)
       IS DISTINCT FROM
       (OLD.risk_run_id, OLD.calc_date, OLD.run_scope, OLD.nonpublishing_reason,
        OLD.policy_id, OLD.policy_version, OLD.policy_hash, OLD.due_session,
        OLD.requested_calc_date, OLD.requested_limit, OLD.universe_digest,
        OLD.feature_definition_version, OLD.started_at, OLD.expected_rows) THEN
        RAISE EXCEPTION 'NAV risk run identity and scope are immutable';
    END IF;
    IF OLD.status = 'running' AND NEW.status = 'metrics_complete' THEN
        SELECT count(*) INTO member_count
        FROM fund_nav_risk_run_members WHERE risk_run_id = OLD.risk_run_id;
        SELECT count(*) INTO uncovered
        FROM fund_nav_risk_run_members m
        WHERE m.risk_run_id = OLD.risk_run_id
          AND NOT EXISTS (SELECT 1 FROM fund_nav_feature_evidence f
                          WHERE f.risk_run_id = m.risk_run_id
                            AND f.instrument_id = m.instrument_id)
          AND NOT EXISTS (SELECT 1 FROM fund_nav_risk_exclusions x
                          WHERE x.risk_run_id = m.risk_run_id
                            AND x.instrument_id = m.instrument_id);
        SELECT count(*) INTO overlapping
        FROM fund_nav_feature_evidence f
        JOIN fund_nav_risk_exclusions x USING (risk_run_id, instrument_id)
        WHERE f.risk_run_id = OLD.risk_run_id;
        IF member_count <> OLD.expected_rows OR uncovered <> 0 OR overlapping <> 0 THEN
            RAISE EXCEPTION 'NAV risk evidence does not cover the exact member set';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status = 'metrics_complete' AND NEW.status = 'complete' THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'NAV risk run status transition is not permitted';
END$$;
DROP TRIGGER IF EXISTS fund_nav_risk_run_guard ON fund_nav_risk_runs;
CREATE TRIGGER fund_nav_risk_run_guard
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_risk_runs
FOR EACH ROW EXECUTE FUNCTION fund_nav_risk_run_guard_v1();

CREATE OR REPLACE FUNCTION fund_nav_risk_evidence_guard_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parent record;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV risk evidence is append-only';
    END IF;
    SELECT status, calc_date, feature_definition_version INTO parent
    FROM fund_nav_risk_runs WHERE risk_run_id = NEW.risk_run_id FOR SHARE;
    IF NOT FOUND THEN
        RETURN NEW;  -- the foreign key reports the missing parent
    END IF;
    IF parent.status <> 'running' THEN
        RAISE EXCEPTION 'NAV risk run is no longer accepting evidence';
    END IF;
    IF TG_TABLE_NAME = 'fund_nav_risk_run_members' THEN
        IF EXISTS (SELECT 1 FROM fund_nav_feature_evidence f
                   WHERE f.risk_run_id = NEW.risk_run_id)
           OR EXISTS (SELECT 1 FROM fund_nav_risk_exclusions x
                      WHERE x.risk_run_id = NEW.risk_run_id) THEN
            RAISE EXCEPTION 'NAV risk member set is frozen once evidence exists';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.calc_date IS DISTINCT FROM parent.calc_date THEN
        RAISE EXCEPTION 'NAV risk evidence calc_date differs from its run';
    END IF;
    IF TG_TABLE_NAME = 'fund_nav_feature_evidence' THEN
        IF NEW.definition_version IS DISTINCT FROM parent.feature_definition_version THEN
            RAISE EXCEPTION 'NAV feature definition differs from its run';
        END IF;
    END IF;
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS fund_nav_risk_member_guard ON fund_nav_risk_run_members;
CREATE TRIGGER fund_nav_risk_member_guard
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_risk_run_members
FOR EACH ROW EXECUTE FUNCTION fund_nav_risk_evidence_guard_v1();
DROP TRIGGER IF EXISTS fund_nav_risk_exclusion_guard ON fund_nav_risk_exclusions;
CREATE TRIGGER fund_nav_risk_exclusion_guard
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_risk_exclusions
FOR EACH ROW EXECUTE FUNCTION fund_nav_risk_evidence_guard_v1();
DROP TRIGGER IF EXISTS fund_nav_feature_evidence_guard ON fund_nav_feature_evidence;
CREATE TRIGGER fund_nav_feature_evidence_guard
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_feature_evidence
FOR EACH ROW EXECUTE FUNCTION fund_nav_risk_evidence_guard_v1();

CREATE TABLE IF NOT EXISTS fund_nav_readiness_runs (
    run_id uuid PRIMARY KEY,
    readiness_profile text NOT NULL CHECK (readiness_profile = 'current_daily_nav_v1'),
    readiness_version integer NOT NULL CHECK (readiness_version = 1),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_hash char(64) NOT NULL,
    calendar_id varchar(128) NOT NULL,
    calendar_version varchar(64) NOT NULL,
    decision_at timestamptz NOT NULL,
    as_of_session date NOT NULL,
    latest_closed_session date NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    sample_id char(64) NOT NULL,
    input_watermark date,
    risk_publication_revision bigint NOT NULL,
    published_risk_run_id uuid NOT NULL REFERENCES fund_nav_risk_runs,
    evaluated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    state text NOT NULL CHECK (state IN ('building','complete')),
    expected_rows integer NOT NULL CHECK (expected_rows >= 0),
    published_rows integer CHECK (published_rows >= 0),
    run_fingerprint char(64),
    UNIQUE (run_id, state),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions,
    CHECK (as_of_session = window_end AND window_start < window_end
           AND latest_closed_session >= as_of_session),
    CHECK ((state = 'building' AND completed_at IS NULL AND published_rows IS NULL)
        OR (state = 'complete' AND completed_at IS NOT NULL
            AND published_rows = expected_rows AND run_fingerprint IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS fund_nav_readiness_v1 (
    run_id uuid NOT NULL REFERENCES fund_nav_readiness_runs,
    instrument_id uuid NOT NULL,
    readiness_profile text NOT NULL CHECK (readiness_profile = 'current_daily_nav_v1'),
    readiness_version integer NOT NULL CHECK (readiness_version = 1),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_hash char(64) NOT NULL,
    calendar_id varchar(128) NOT NULL,
    calendar_version varchar(64) NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    as_of_session date GENERATED ALWAYS AS (window_end) STORED,
    fund_status text NOT NULL CHECK (fund_status IN ('ACTIVE','INACTIVE','UNKNOWN')),
    is_active boolean,
    status_known_at timestamptz,
    status_effective_at timestamptz,
    lifecycle_evidence_id uuid,
    valuation_frequency text NOT NULL,
    last_nav date,
    nav_last_date date GENERATED ALWAYS AS (last_nav) STORED,
    first_nav date,
    last_usable_return_end date,
    is_current boolean NOT NULL,
    missed_due_sessions integer NOT NULL CHECK (missed_due_sessions >= 0),
    missing_session_count integer NOT NULL CHECK (missing_session_count >= 0),
    observed_levels_count integer NOT NULL,
    observed_endpoint_count integer GENERATED ALWAYS AS (observed_levels_count) STORED,
    admissible_returns_count integer NOT NULL,
    observed_return_count integer GENERATED ALWAYS AS (admissible_returns_count) STORED,
    interval_compatible boolean NOT NULL,
    return_semantics text,
    identity_verified boolean NOT NULL,
    return_basis_verified boolean NOT NULL,
    currency_verified boolean NOT NULL,
    ingestion_run_id uuid,
    feature_evidence_id text,
    risk_run_id uuid,
    risk_input_fingerprint char(64),
    nav_revision_id bigint NOT NULL CHECK (nav_revision_id >= 0),
    risk_input_max_date date,
    feature_as_of date,
    cohort text,
    admissible boolean NOT NULL,
    ready boolean GENERATED ALWAYS AS (admissible) STORED,
    reason_code text CHECK (reason_code IN (
        'INACTIVE_FUND','UNKNOWN_FUND_STATUS','NAV_STALE',
        'UNSUPPORTED_VALUATION_FREQUENCY','NAV_POLICY_UNAVAILABLE',
         'NAV_DATA_UNAVAILABLE','NAV_RETURN_SEMANTICS_UNSUPPORTED',
         'RETURN_INTERVAL_INCOMPATIBLE',
        'RETURN_SAMPLE_NOT_CURRENT')),
    input_fingerprint char(64) NOT NULL,
    evidence_digest char(64) GENERATED ALWAYS AS (input_fingerprint) STORED,
    sample_id char(64) NOT NULL,
    -- SHA256 of the ordered per-session calendar proof used for the 401 levels
    -- (observed stamp -> current session, including rollover-equivalent stamps).
    calendar_equivalence_digest char(64)
        CHECK (calendar_equivalence_digest ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, instrument_id),
    -- COALESCE: an admissible row must satisfy every term; NULL is never a pass.
    CHECK (CASE WHEN admissible THEN COALESCE(
               reason_code IS NULL AND is_active IS TRUE AND fund_status = 'ACTIVE'
               AND valuation_frequency = 'daily'
               AND is_current AND interval_compatible AND missed_due_sessions = 0
               AND missing_session_count = 0
               AND observed_levels_count = 401 AND admissible_returns_count = 400
               AND return_semantics = 'observed_interval_log_ratio'
               AND identity_verified AND return_basis_verified AND currency_verified
               AND lifecycle_evidence_id IS NOT NULL
               AND ingestion_run_id IS NOT NULL
               AND risk_run_id IS NOT NULL
               AND feature_evidence_id IS NOT NULL
               AND length(btrim(feature_evidence_id)) > 0
               AND risk_input_fingerprint ~ '^[0-9a-f]{64}$'
               AND input_fingerprint ~ '^[0-9a-f]{64}$'
               AND calendar_equivalence_digest IS NOT NULL
               AND risk_input_max_date IS NOT NULL AND feature_as_of IS NOT NULL
               AND risk_input_max_date = window_end AND feature_as_of = window_end,
               false)
           ELSE reason_code IS NOT NULL END)
);
CREATE INDEX IF NOT EXISTS fund_nav_readiness_admission_idx
    ON fund_nav_readiness_v1 (run_id, policy_version, admissible, window_end, instrument_id);

CREATE TABLE IF NOT EXISTS fund_nav_readiness_current (
    readiness_profile text PRIMARY KEY CHECK (readiness_profile = 'current_daily_nav_v1'),
    run_id uuid NOT NULL,
    state text NOT NULL DEFAULT 'complete' CHECK (state = 'complete'),
    published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (run_id, state) REFERENCES fund_nav_readiness_runs (run_id, state)
);

CREATE OR REPLACE FUNCTION fund_nav_readiness_freeze_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parent_state text;
    parent_run_id uuid;
BEGIN
    IF TG_TABLE_NAME = 'fund_nav_readiness_runs' THEN
        IF TG_OP = 'DELETE' OR OLD.state = 'complete' THEN
            RAISE EXCEPTION 'completed NAV readiness run is immutable';
        END IF;
    ELSE
        IF TG_OP = 'INSERT' THEN
            parent_run_id := NEW.run_id;
        ELSE
            parent_run_id := OLD.run_id;
        END IF;
        SELECT state INTO parent_state FROM fund_nav_readiness_runs
        WHERE run_id = parent_run_id;
        IF parent_state = 'complete' THEN
            RAISE EXCEPTION 'completed NAV readiness rows are immutable';
        END IF;
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END$$;
DROP TRIGGER IF EXISTS fund_nav_readiness_run_freeze ON fund_nav_readiness_runs;
CREATE TRIGGER fund_nav_readiness_run_freeze
BEFORE UPDATE OR DELETE ON fund_nav_readiness_runs
FOR EACH ROW EXECUTE FUNCTION fund_nav_readiness_freeze_v1();
DROP TRIGGER IF EXISTS fund_nav_readiness_row_freeze ON fund_nav_readiness_v1;
CREATE TRIGGER fund_nav_readiness_row_freeze
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_readiness_v1
FOR EACH ROW EXECUTE FUNCTION fund_nav_readiness_freeze_v1();

-- Explicit as_of permits deterministic lifecycle/coverage boundary tests. No
-- HTTP attempt or computed_at timestamp is used as a publication generation.
CREATE OR REPLACE FUNCTION fund_nav_snapshot_current_at_v1(
    subject_id uuid, selected_run_id uuid, evaluated_at timestamptz
) RETURNS boolean LANGUAGE sql STABLE AS $$
SELECT COALESCE((
    SELECT run.state='complete' AND $3 >= run.completed_at
       AND $3 <= policy.valid_through
       AND policy.policy_hash = run.policy_hash
       AND policy.published_at <= $3
       AND due.session_date = run.as_of_session
       AND closed.session_date = run.latest_closed_session
       AND due.session_date BETWEEN policy.coverage_start AND policy.coverage_end
       AND active.evidence_id IS NOT DISTINCT FROM r.lifecycle_evidence_id
       AND COALESCE(nav_head.revision_id, 0) = r.nav_revision_id
       AND risk_pub.state = 'idle'
       AND risk_pub.revision_id = run.risk_publication_revision
       AND risk_pub.published_risk_run_id = run.published_risk_run_id
       AND published_risk.status = 'complete'
       AND published_risk.run_scope = 'current_full'
       AND (NOT r.admissible OR (
            r.risk_run_id = risk_pub.published_risk_run_id
            AND NOT EXISTS (
                SELECT 1 FROM fund_nav_reexpression_holds hold
                WHERE hold.instrument_id = r.instrument_id
            )
            AND EXISTS (
                SELECT 1 FROM fund_nav_feature_evidence feature
                WHERE feature.risk_run_id = r.risk_run_id
                  AND feature.instrument_id = r.instrument_id
                  AND feature.calc_date::text = split_part(r.feature_evidence_id, ':', 2)
                  AND feature.definition_version = split_part(r.feature_evidence_id, ':', 3)
                  AND feature.definition_version = published_risk.feature_definition_version
                  AND feature.feature_as_of = r.feature_as_of
                  AND feature.input_max_date = r.risk_input_max_date
                  AND feature.input_fingerprint = r.risk_input_fingerprint
            )
       ))
    FROM fund_nav_readiness_current p
    JOIN fund_nav_readiness_runs run ON run.run_id = p.run_id
    JOIN fund_nav_readiness_v1 r ON r.run_id = run.run_id AND r.instrument_id = $1
    JOIN nav_policy_versions policy
      ON policy.policy_id = run.policy_id AND policy.policy_version = run.policy_version
    JOIN nav_policy_current active_policy
      ON active_policy.readiness_profile = run.readiness_profile
     AND active_policy.policy_id = run.policy_id
     AND active_policy.policy_version = run.policy_version
    LEFT JOIN fund_nav_data_heads nav_head ON nav_head.instrument_id = r.instrument_id
    JOIN fund_nav_risk_publication risk_pub
      ON risk_pub.readiness_profile = run.readiness_profile
    JOIN fund_nav_risk_runs published_risk
      ON published_risk.risk_run_id = run.published_risk_run_id
    LEFT JOIN LATERAL (
        SELECT s.session_date FROM nav_valuation_schedules s
        WHERE s.calendar_id = run.calendar_id AND s.calendar_version = run.calendar_version
          AND s.nav_due_at <= $3
        ORDER BY s.session_date DESC LIMIT 1
    ) due ON true
    LEFT JOIN LATERAL (
        SELECT s.session_date FROM nav_valuation_schedules s
        WHERE s.calendar_id = run.calendar_id AND s.calendar_version = run.calendar_version
          AND s.valuation_close_at <= $3
        ORDER BY s.session_date DESC LIMIT 1
    ) closed ON true
    LEFT JOIN LATERAL (
        SELECT e.evidence_id FROM nav_instrument_policy_evidence e
        WHERE e.instrument_id = r.instrument_id AND e.policy_id = r.policy_id
          AND e.policy_version = r.policy_version
          AND e.known_at <= $3 AND e.effective_at <= $3
        ORDER BY e.effective_at DESC, e.known_at DESC LIMIT 1
    ) active ON true
    WHERE p.readiness_profile='current_daily_nav_v1'
      AND p.run_id=$2
    LIMIT 1
), false)
$$ SECURITY DEFINER SET search_path FROM CURRENT;
REVOKE ALL ON FUNCTION fund_nav_snapshot_current_at_v1(uuid,uuid,timestamptz)
    FROM PUBLIC;

CREATE OR REPLACE VIEW fund_nav_readiness_current_v1 AS
SELECT r.*, p.published_at AS pointer_published_at,
       run.decision_at, run.completed_at, run.run_fingerprint,
       run.latest_closed_session, run.risk_publication_revision,
       due.session_date AS latest_due_session,
       fund_nav_snapshot_current_at_v1(r.instrument_id,run.run_id,clock_timestamp())
           AS snapshot_current
FROM fund_nav_readiness_current p
JOIN fund_nav_readiness_runs run ON run.run_id = p.run_id
JOIN fund_nav_readiness_v1 r ON r.run_id = run.run_id
LEFT JOIN LATERAL (
    SELECT s.session_date FROM nav_valuation_schedules s
    WHERE s.calendar_id = run.calendar_id AND s.calendar_version = run.calendar_version
      AND s.nav_due_at <= clock_timestamp()
    ORDER BY s.session_date DESC LIMIT 1
) due ON true
WHERE p.readiness_profile = 'current_daily_nav_v1' AND run.state = 'complete';

-- Read-model grants only. The view executes with its owner privileges and does
-- not expose provider attempt/lifecycle/risk evidence relations to app_runtime.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_runtime') THEN
        GRANT SELECT ON nav_policy_versions, nav_policy_current,
            nav_valuation_schedules, fund_nav_readiness_runs,
            fund_nav_readiness_v1, fund_nav_readiness_current,
            fund_nav_readiness_current_v1 TO app_runtime;
        GRANT EXECUTE ON FUNCTION fund_nav_snapshot_current_at_v1(uuid,uuid,timestamptz)
            TO app_runtime;
    END IF;
END$$;

COMMIT;
