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

-- A row revision is recorded in the *same* transaction as any NAV/return change,
-- regardless of writer (ingestion, operator or reprocessor). The head row lock
-- serializes revision IDs for one instrument even under concurrent transactions.
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
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_instrument_idx
    ON fund_nav_data_revisions (instrument_id, revision_id DESC);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_window_idx
    ON fund_nav_data_revisions (instrument_id, nav_date, revision_id DESC);
CREATE OR REPLACE FUNCTION fund_nav_revision_append_only_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'NAV data revisions are append-only';
END$$;
DROP TRIGGER IF EXISTS fund_nav_revision_append_only ON fund_nav_data_revisions;
CREATE TRIGGER fund_nav_revision_append_only
BEFORE UPDATE OR DELETE ON fund_nav_data_revisions
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
CREATE OR REPLACE FUNCTION fund_nav_stamp_revision_v1() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    affected_instrument uuid;
    affected_date date;
    new_revision bigint;
    run_setting text;
BEGIN
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
    ELSE
        affected_instrument := NEW.instrument_id;
        affected_date := NEW.nav_date;
    END IF;
    INSERT INTO fund_nav_data_heads (instrument_id) VALUES (affected_instrument)
    ON CONFLICT (instrument_id) DO NOTHING;
    PERFORM 1 FROM fund_nav_data_heads WHERE instrument_id = affected_instrument FOR UPDATE;
    run_setting := current_setting('nav.ingestion_run_id', true);
    INSERT INTO fund_nav_data_revisions
        (instrument_id, nav_date, mutation_kind, source_run_id)
    VALUES (affected_instrument, affected_date, TG_OP,
            NULLIF(run_setting, '')::uuid)
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
CREATE TABLE IF NOT EXISTS fund_nav_risk_runs (
    risk_run_id uuid PRIMARY KEY,
    calc_date date NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN ('running','metrics_complete','complete')),
    expected_rows integer NOT NULL CHECK (expected_rows >= 0),
    persisted_rows integer CHECK (persisted_rows >= 0),
    CHECK ((status = 'running' AND completed_at IS NULL AND persisted_rows IS NULL)
        OR (status = 'metrics_complete' AND completed_at IS NULL
            AND persisted_rows = expected_rows)
        OR (status = 'complete' AND completed_at IS NOT NULL
            AND persisted_rows = expected_rows))
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
CREATE TABLE IF NOT EXISTS fund_nav_risk_exclusions (
    risk_run_id uuid NOT NULL REFERENCES fund_nav_risk_runs,
    instrument_id uuid NOT NULL,
    calc_date date NOT NULL,
    reason_code text NOT NULL CHECK (reason_code IN ('NAV_WINDOW_TOO_SHORT','METRICS_UNAVAILABLE')),
    nav_count integer NOT NULL CHECK (nav_count >= 0),
    input_max_date date,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (risk_run_id, instrument_id)
);
CREATE INDEX IF NOT EXISTS fund_nav_risk_exclusions_recent_idx
    ON fund_nav_risk_exclusions (instrument_id, calc_date DESC);
CREATE TABLE IF NOT EXISTS fund_nav_feature_evidence (
    instrument_id uuid NOT NULL,
    calc_date date NOT NULL,
    definition_version text NOT NULL,
    risk_run_id uuid NOT NULL REFERENCES fund_nav_risk_runs,
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
    PRIMARY KEY (instrument_id, calc_date, definition_version),
    CHECK (nav_start <= nav_end AND nav_end = input_max_date)
);
CREATE INDEX IF NOT EXISTS fund_nav_feature_evidence_latest_idx
    ON fund_nav_feature_evidence (instrument_id, feature_as_of DESC);

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
    PRIMARY KEY (run_id, instrument_id),
    CHECK ((admissible AND reason_code IS NULL AND is_active IS TRUE
           AND is_current AND interval_compatible AND missed_due_sessions = 0
           AND missing_session_count = 0
           AND observed_levels_count = 401 AND admissible_returns_count = 400
           AND identity_verified AND return_basis_verified AND currency_verified
           AND risk_input_max_date = window_end AND feature_as_of = window_end
           AND lifecycle_evidence_id IS NOT NULL AND risk_run_id IS NOT NULL
           AND risk_input_fingerprint IS NOT NULL)
        OR (NOT admissible AND reason_code IS NOT NULL))
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
       AND (NOT r.admissible OR (
            r.risk_run_id = risk_pub.published_risk_run_id
            AND NOT EXISTS (
                SELECT 1 FROM fund_nav_reexpression_holds hold
                WHERE hold.instrument_id = r.instrument_id
            )
            AND EXISTS (
                SELECT 1 FROM fund_nav_feature_evidence feature
                WHERE feature.instrument_id = r.instrument_id
                  AND feature.risk_run_id = r.risk_run_id
                  AND feature.calc_date::text = split_part(r.feature_evidence_id, ':', 2)
                  AND feature.definition_version = split_part(r.feature_evidence_id, ':', 3)
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
