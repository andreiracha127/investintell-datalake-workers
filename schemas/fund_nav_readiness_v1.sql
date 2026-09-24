-- Additive, versioned NAV evidence. Apply only to an explicitly selected schema.
-- PR132's nav_timeseries provenance upgrade is a prerequisite, not repeated here.
-- The operator applies this file with `SET search_path TO <schema>, pg_temp`;
-- every function captures exactly that path (`SET search_path FROM CURRENT`),
-- so pg_temp is searched last and a temporary object cannot shadow W1 state.
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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    calendar_key varchar(128);
    version_key varchar(64);
BEGIN
    IF TG_TABLE_NAME = 'nav_policy_versions' THEN
        IF TG_OP = 'INSERT' THEN
            IF NEW.published_at IS NOT NULL THEN
                RAISE EXCEPTION 'NAV policy must be inserted unpublished';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' OR OLD.published_at IS NOT NULL THEN
            RAISE EXCEPTION 'published NAV policy is immutable';
        END IF;
        -- Publication instant is the server clock, never a caller value.
        IF NEW.published_at IS NOT NULL THEN
            NEW.published_at := clock_timestamp();
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
CREATE TRIGGER nav_policy_freeze BEFORE INSERT OR UPDATE OR DELETE ON nav_policy_versions
FOR EACH ROW EXECUTE FUNCTION nav_policy_freeze_v1();
DROP TRIGGER IF EXISTS nav_schedule_freeze ON nav_valuation_schedules;
CREATE TRIGGER nav_schedule_freeze BEFORE INSERT OR UPDATE OR DELETE ON nav_valuation_schedules
FOR EACH ROW EXECUTE FUNCTION nav_policy_freeze_v1();

-- N3: pointer instants are server-controlled. Any real INSERT/UPDATE is stamped
-- with the current server clock (a caller value is overwritten), a regressed
-- clock aborts instead of back-dating, and the target must already be published.
-- Re-pointing to an older version is allowed and receives a new instant.
CREATE OR REPLACE FUNCTION nav_policy_pointer_stamp_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    stamp timestamptz := clock_timestamp();
    target_published timestamptz;
BEGIN
    IF TG_OP = 'UPDATE' AND stamp <= OLD.published_at THEN
        RAISE EXCEPTION 'publication_clock_regressed';
    END IF;
    SELECT p.published_at INTO target_published FROM nav_policy_versions p
    WHERE p.policy_id = NEW.policy_id AND p.policy_version = NEW.policy_version
      AND p.readiness_profile = NEW.readiness_profile;
    IF target_published IS NULL OR target_published > stamp THEN
        RAISE EXCEPTION 'NAV policy pointer target is not published';
    END IF;
    NEW.published_at := stamp;
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_policy_pointer_stamp ON nav_policy_current;
CREATE TRIGGER nav_policy_pointer_stamp BEFORE INSERT OR UPDATE ON nav_policy_current
FOR EACH ROW EXECUTE FUNCTION nav_policy_pointer_stamp_v1();

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
-- recorded_at is the server instant the fact became known to W (N3 filter).
CREATE OR REPLACE FUNCTION nav_instrument_evidence_append_only_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.recorded_at := clock_timestamp();
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'NAV lifecycle evidence is append-only';
END$$;
DROP TRIGGER IF EXISTS nav_instrument_evidence_append_only ON nav_instrument_policy_evidence;
CREATE TRIGGER nav_instrument_evidence_append_only
BEFORE INSERT OR UPDATE OR DELETE ON nav_instrument_policy_evidence
FOR EACH ROW EXECUTE FUNCTION nav_instrument_evidence_append_only_v1();

-- Canonical digest of the WHOLE lifecycle partition (policy_id, policy_version):
-- every row, all 13 columns including the server identities evidence_id and
-- recorded_at, as one JSON array per row (fixed field order, JSON escaping,
-- JSON booleans) joined by LF without a terminator, ordered by native
-- uuid/timestamptz values (never text/collation), instants rendered in UTC
-- with six fractional digits and an explicit AD/BC era (infinities as explicit
-- tokens), so TimeZone/DateStyle/lc_time never change it. An empty partition
-- is the SHA-256 of zero bytes. Private and SECURITY INVOKER: pinned
-- search_path (<schema>, pg_temp), the table qualified by the target schema and
-- digest() qualified by the namespace of the pgcrypto extension, resolved and
-- verified (extension membership) here. pgcrypto is a prerequisite: never
-- installed or moved by this file.
DO $do$
DECLARE
    crypto_schema name;
BEGIN
    SELECT n.nspname INTO crypto_schema
      FROM pg_catalog.pg_extension x
      JOIN pg_catalog.pg_namespace n ON n.oid = x.extnamespace
      JOIN pg_catalog.pg_proc p
        ON p.pronamespace = n.oid AND p.proname = 'digest'
       AND pg_catalog.oidvectortypes(p.proargtypes) = 'bytea, text'
      JOIN pg_catalog.pg_depend d
        ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid = p.oid
       AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
       AND d.refobjid = x.oid AND d.deptype = 'e'
     WHERE x.extname = 'pgcrypto';
    IF crypto_schema IS NULL THEN
        RAISE EXCEPTION 'pgcrypto digest(bytea, text) is a required prerequisite';
    END IF;
    EXECUTE pg_catalog.format($fmt$
CREATE OR REPLACE FUNCTION nav_policy_evidence_digest_v1(text, text) RETURNS text
LANGUAGE sql STABLE SECURITY INVOKER SET search_path FROM CURRENT AS $body$
SELECT pg_catalog.encode(%1$I.digest(pg_catalog.convert_to(
  COALESCE(pg_catalog.string_agg(
    pg_catalog.jsonb_build_array(
      e.evidence_id::text, e.instrument_id::text,
      e.policy_id, e.policy_version,
      CASE WHEN pg_catalog.isfinite(e.known_at) THEN
        pg_catalog.to_char(e.known_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z" AD')
        WHEN e.known_at = 'infinity'::timestamptz THEN 'infinity' ELSE '-infinity' END,
      CASE WHEN pg_catalog.isfinite(e.effective_at) THEN
        pg_catalog.to_char(e.effective_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z" AD')
        WHEN e.effective_at = 'infinity'::timestamptz THEN 'infinity' ELSE '-infinity' END,
      e.fund_status, e.valuation_frequency,
      e.identity_verified, e.return_basis_verified, e.currency_verified,
      e.evidence_reference,
      CASE WHEN pg_catalog.isfinite(e.recorded_at) THEN
        pg_catalog.to_char(e.recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z" AD')
        WHEN e.recorded_at = 'infinity'::timestamptz THEN 'infinity' ELSE '-infinity' END
    )::text, E'\n'
    ORDER BY e.instrument_id, e.known_at, e.effective_at, e.evidence_id
  ), ''), 'UTF8'), 'sha256'), 'hex')
FROM %2$I.nav_instrument_policy_evidence e
WHERE e.policy_id = $1 AND e.policy_version = $2
$body$
$fmt$, crypto_schema, pg_catalog.current_schema());
END$do$;

-- Governed publication receipt (operator plan v4): one private, append-only
-- row per committed publication, written in the same transaction as the
-- pointer move. It binds the normalized plan digest, the policy bytes and
-- document, and the audit receipt, AND the publication event: the current
-- pointer instant it certifies and the server digest of the whole lifecycle
-- partition as persisted at that instant (the transaction's own writes
-- included). The server stamps the instant, xid, pointer instant and digest;
-- the row must describe the current, published, unexpired pointer, and the
-- instant must lie inside the audited capture/SEC-freshness window. At most
-- one receipt per pointer event. Additive: absent on older W1 schemas.
--
-- Admission of an existing ledger, BEFORE anything below touches it, decided
-- under ACCESS EXCLUSIVE and inside this file's single transaction (the
-- shared DDL itself refuses; the operator's check is not the only guard).
-- Three fingerprints: the table (columns, defaults, constraints, indexes), the
-- guard function semantics (language, security, volatility, strictness,
-- leakproof, parallel, result, pinned search_path, body, no EXECUTE beyond the
-- owner) and the trigger. Admitted: no ledger (fresh creation below); the
-- exact Round4 ledger (trigger present or recreatable, no ALTER); the exact
-- Round3 ledger (local/dev) while EMPTY, upgraded in place. Anything else, or
-- any Round3 receipt, raises SQLSTATE NV409 and the whole transaction rolls
-- back: receipts are never truncated, backfilled or weakened.
DO $do$
DECLARE
    receipts regclass := pg_catalog.to_regclass(pg_catalog.format(
        '%I.nav_policy_publication_receipts', pg_catalog.current_schema()));
    guard regprocedure := pg_catalog.to_regprocedure(pg_catalog.format(
        '%I.nav_policy_publication_receipt_guard_v1()', pg_catalog.current_schema()));
    table_fp text;
    guard_fp text;
    trigger_fp text;
BEGIN
    IF receipts IS NULL THEN
        RETURN;
    END IF;
    EXECUTE pg_catalog.format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', receipts);
    -- fingerprint:begin
    table_fp := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
        pg_catalog.concat_ws(E'\n#\n',
            COALESCE((SELECT pg_catalog.string_agg(pg_catalog.concat_ws(' ', a.attname,
                        pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull,
                        pg_catalog.pg_get_expr(d.adbin, d.adrelid)), E'\n' ORDER BY a.attnum)
               FROM pg_catalog.pg_attribute a
               LEFT JOIN pg_catalog.pg_attrdef d
                 ON d.adrelid = a.attrelid AND d.adnum = a.attnum
              WHERE a.attrelid = receipts AND a.attnum > 0 AND NOT a.attisdropped), '-'),
            COALESCE((SELECT pg_catalog.string_agg(k.def, E'\n' ORDER BY k.def COLLATE "C")
               FROM (SELECT c.contype::text || ' '
                            || pg_catalog.pg_get_constraintdef(c.oid, true) AS def
                       FROM pg_catalog.pg_constraint c WHERE c.conrelid = receipts) k), '-'),
            COALESCE((SELECT pg_catalog.string_agg(k.def, E'\n' ORDER BY k.def COLLATE "C")
               FROM (SELECT pg_catalog.replace(pg_catalog.pg_get_indexdef(i.indexrelid),
                            ' ON ' || pg_catalog.quote_ident(pg_catalog.current_schema()) || '.',
                            ' ON ') AS def
                       FROM pg_catalog.pg_index i WHERE i.indrelid = receipts) k), '-')
        ), 'UTF8')), 'hex');
    guard_fp := COALESCE((
        SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            pg_catalog.concat_ws(E'\n', l.lanname, p.prosecdef::text, p.provolatile::text,
                p.proisstrict::text, p.proleakproof::text, p.proparallel::text,
                pg_catalog.format_type(p.prorettype, NULL),
                COALESCE(pg_catalog.array_to_string(ARRAY(
                    SELECT pg_catalog.replace(setting,
                               'search_path=' || pg_catalog.quote_ident(pg_catalog.current_schema()) || ',',
                               'search_path=@schema@,')
                      FROM pg_catalog.unnest(p.proconfig) AS setting), E'\x1f'), '-'),
                pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc, 'UTF8')), 'hex'),
                (NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(
                     COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) g
                   WHERE g.grantee <> p.proowner))::text),
            'UTF8')), 'hex')
          FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid = p.prolang
         WHERE p.oid = guard), 'none');
    trigger_fp := COALESCE((
        SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            pg_catalog.string_agg(k.def, E'\n' ORDER BY k.def COLLATE "C"), 'UTF8')), 'hex')
          FROM (SELECT t.tgname::text || ' ' || t.tgenabled::text || ' '
                       || pg_catalog.pg_get_triggerdef(t.oid, true) AS def
                  FROM pg_catalog.pg_trigger t
                 WHERE t.tgrelid = receipts AND NOT t.tgisinternal) k), 'none');
    -- fingerprint:end
    IF table_fp = '156431a42f90b7d77adea6a11332a9b88601f49e709a527bae1e0ee491055fa0'
       AND guard_fp = '4bb299b36b8871336072a578936e9812f665b6d30181e0a6b2ec271ba8c5f4f8'
       AND trigger_fp IN ('18c21954c9d3bcc74a5f60120cffc7689a2903bdf89ada3f5d3946a9d4f0b239',
                          'none') THEN
        RETURN;
    END IF;
    IF table_fp IS DISTINCT FROM '0191bc90ee1cb1bd5612717db5cc51c396644ba0b3fd13c50cfca98231b33432'
       OR guard_fp IS DISTINCT FROM '7e7db3ddc00e27892a1d2578d3f812b5259d90a652fb824d158851bd764f70cb'
       OR trigger_fp IS DISTINCT FROM '18c21954c9d3bcc74a5f60120cffc7689a2903bdf89ada3f5d3946a9d4f0b239' THEN
        RAISE EXCEPTION USING ERRCODE = 'NV409',
            MESSAGE = 'nav_policy_publication_receipts is not an admissible ledger';
    END IF;
    IF EXISTS (SELECT 1 FROM nav_policy_publication_receipts) THEN
        RAISE EXCEPTION USING ERRCODE = 'NV409',
            MESSAGE = 'Round3 publication receipts are not empty; no upgrade';
    END IF;
    ALTER TABLE nav_policy_publication_receipts
        ADD COLUMN pointer_published_at timestamptz NOT NULL,
        ADD COLUMN evidence_partition_digest char(64) NOT NULL
            CHECK (evidence_partition_digest ~ '^[0-9a-f]{64}$'),
        ADD CONSTRAINT nav_policy_publication_receipts_event_key
            UNIQUE (readiness_profile, policy_id, policy_version, pointer_published_at),
        ADD CHECK (pointer_published_at <= published_at);
END$do$;
CREATE TABLE IF NOT EXISTS nav_policy_publication_receipts (
    receipt_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    readiness_profile text NOT NULL CHECK (readiness_profile = 'current_daily_nav_v1'),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_hash char(64) NOT NULL CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
    plan_version text NOT NULL CHECK (plan_version = 'nav-schema-plan-v4'),
    plan_sha256 char(64) NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    policy_artifact_sha256 char(64) NOT NULL
        CHECK (policy_artifact_sha256 ~ '^[0-9a-f]{64}$'),
    policy_document_digest char(64) NOT NULL
        CHECK (policy_document_digest ~ '^[0-9a-f]{64}$'),
    audit_receipt_sha256 char(64) NOT NULL CHECK (audit_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    audit_dossier_sha256 char(64) NOT NULL CHECK (audit_dossier_sha256 ~ '^[0-9a-f]{64}$'),
    canary_manifest_sha256 char(64) NOT NULL
        CHECK (canary_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    capture_bundle_sha256 char(64) NOT NULL
        CHECK (capture_bundle_sha256 ~ '^[0-9a-f]{64}$'),
    previous_policy_id text,
    previous_policy_version text,
    previous_policy_hash char(64) CHECK (previous_policy_hash ~ '^[0-9a-f]{64}$'),
    captured_at timestamptz NOT NULL,
    sec_valid_until timestamptz NOT NULL,
    published_at timestamptz NOT NULL,
    commit_xid xid8 NOT NULL,
    pointer_published_at timestamptz NOT NULL,
    evidence_partition_digest char(64) NOT NULL
        CHECK (evidence_partition_digest ~ '^[0-9a-f]{64}$'),
    UNIQUE (plan_sha256),
    CONSTRAINT nav_policy_publication_receipts_event_key
        UNIQUE (readiness_profile, policy_id, policy_version, pointer_published_at),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions,
    CHECK ((previous_policy_id IS NULL) = (previous_policy_version IS NULL)
           AND (previous_policy_id IS NULL) = (previous_policy_hash IS NULL)),
    CHECK (captured_at <= published_at AND published_at <= sec_valid_until),
    CHECK (pointer_published_at <= published_at)
);
CREATE INDEX IF NOT EXISTS nav_policy_publication_receipts_policy_idx
    ON nav_policy_publication_receipts (policy_id, policy_version, published_at DESC);
-- The guard reads the target pointer + published version (FOR SHARE OF the
-- pointer, compatible with the operator's own write lock), requires the
-- submitted identity/profile/hash to be the current published unexpired
-- policy at the final server stamp greatest(clock, pointer instant), and
-- ALWAYS overwrites the four server fields (never COALESCEs a caller value).
-- The capture/SEC window is enforced against that stamp by the table CHECKs.
CREATE OR REPLACE FUNCTION nav_policy_publication_receipt_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    pointer_stamp timestamptz;
    target_hash char(64);
    target_published timestamptz;
    target_valid_through timestamptz;
    stamp timestamptz;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV policy publication receipts are append-only';
    END IF;
    SELECT c.published_at, p.policy_hash, p.published_at, p.valid_through
      INTO pointer_stamp, target_hash, target_published, target_valid_through
      FROM nav_policy_current c
      JOIN nav_policy_versions p
        ON p.policy_id = c.policy_id AND p.policy_version = c.policy_version
     WHERE c.readiness_profile = NEW.readiness_profile
       AND c.policy_id = NEW.policy_id AND c.policy_version = NEW.policy_version
       FOR SHARE OF c;
    stamp := greatest(clock_timestamp(), pointer_stamp);
    IF pointer_stamp IS NULL OR target_hash IS DISTINCT FROM NEW.policy_hash
       OR target_published IS NULL OR target_valid_through < stamp THEN
        RAISE EXCEPTION 'NAV publication receipt must describe the current published unexpired policy';
    END IF;
    NEW.pointer_published_at := pointer_stamp;
    NEW.evidence_partition_digest := nav_policy_evidence_digest_v1(NEW.policy_id, NEW.policy_version);
    NEW.published_at := stamp;
    NEW.commit_xid := pg_current_xact_id();
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_policy_publication_receipt_guard ON nav_policy_publication_receipts;
CREATE TRIGGER nav_policy_publication_receipt_guard
BEFORE INSERT OR UPDATE OR DELETE ON nav_policy_publication_receipts
FOR EACH ROW EXECUTE FUNCTION nav_policy_publication_receipt_guard_v1();

-- A run is a batch envelope, not economic evidence: per-instrument success is
-- proven by the attempt committed in the same transaction as its NAV writes.
CREATE TABLE IF NOT EXISTS nav_ingestion_runs (
    run_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    requested_end date NOT NULL,
    status text NOT NULL CHECK (status IN ('running','completed','aborted','failed')),
    operation text NOT NULL DEFAULT 'normal' CHECK (operation IN ('normal','rebase')),
    contract_version text,
    plan_sha256 char(64) CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    reason_code varchar(48) CHECK (reason_code IS NULL OR reason_code ~ '^[A-Z_]{1,48}$'),
    CHECK ((status = 'running') = (completed_at IS NULL)),
    CHECK (status <> 'running' OR reason_code IS NULL),
    CHECK ((operation = 'normal' AND contract_version IS NULL AND plan_sha256 IS NULL)
        OR (operation = 'rebase' AND contract_version = 'w1-tiingo-adjusted-daily-v1'
            AND plan_sha256 IS NOT NULL))
);
CREATE OR REPLACE FUNCTION nav_ingestion_run_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'running' THEN
            RAISE EXCEPTION 'NAV ingestion run must be registered running';
        END IF;
        NEW.started_at := clock_timestamp();
        NEW.completed_at := NULL;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' OR OLD.status <> 'running' THEN
        RAISE EXCEPTION 'terminal NAV ingestion run is immutable';
    END IF;
    IF NEW.status NOT IN ('completed','aborted','failed')
       OR (NEW.run_id, NEW.started_at, NEW.requested_end, NEW.operation,
           NEW.contract_version, NEW.plan_sha256)
          IS DISTINCT FROM
          (OLD.run_id, OLD.started_at, OLD.requested_end, OLD.operation,
           OLD.contract_version, OLD.plan_sha256) THEN
        RAISE EXCEPTION 'NAV ingestion run may only transition running to terminal';
    END IF;
    NEW.completed_at := clock_timestamp();
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_ingestion_run_guard ON nav_ingestion_runs;
CREATE TRIGGER nav_ingestion_run_guard
BEFORE INSERT OR UPDATE OR DELETE ON nav_ingestion_runs
FOR EACH ROW EXECUTE FUNCTION nav_ingestion_run_guard_v1();

-- One persisted, append-only attempt per (run, instrument, provider). The server
-- assigns commit_xid/persisted_at; a success must be a completed, ordered fetch
-- with rows. Provider-attributed NAV revisions must reference an attempt with
-- the same xid (fund_nav_revision_attribution_v1), never an older attempt.
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
    commit_xid xid8 NOT NULL,
    PRIMARY KEY (run_id, instrument_id, provider),
    CHECK (requested_start <= requested_end),
    CHECK (finished_at IS NULL OR attempted_at IS NULL OR finished_at >= attempted_at),
    CHECK (reason_code IS NULL OR reason_code ~ '^[A-Z_]{1,48}$'),
    CHECK (status NOT IN ('success_new','success_no_new') OR COALESCE(
        attempted_at IS NOT NULL AND finished_at IS NOT NULL
        AND finished_at >= attempted_at AND persisted_at >= finished_at
        AND row_count > 0 AND reason_code IS NULL, false))
);
CREATE INDEX IF NOT EXISTS nav_ingestion_attempts_recent_idx
    ON nav_ingestion_attempts (instrument_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS nav_ingestion_attempts_persisted_idx
    ON nav_ingestion_attempts (instrument_id, persisted_at DESC);
CREATE OR REPLACE FUNCTION nav_ingestion_attempt_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV ingestion attempts are append-only';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM nav_ingestion_runs r
                   WHERE r.run_id = NEW.run_id AND r.status = 'running') THEN
        RAISE EXCEPTION 'NAV ingestion attempt requires a running parent run';
    END IF;
    NEW.commit_xid := pg_current_xact_id();
    NEW.persisted_at := clock_timestamp();
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_ingestion_attempt_guard ON nav_ingestion_attempts;
CREATE TRIGGER nav_ingestion_attempt_guard
BEFORE INSERT OR UPDATE OR DELETE ON nav_ingestion_attempts
FOR EACH ROW EXECUTE FUNCTION nav_ingestion_attempt_guard_v1();

-- nav-level-evidence-v1: canonical digest of one persisted level projection
-- (date, nav, source_nav, source, kind, currency, repair); never calendar or
-- derived return. Mirrored by _nav_policy.level_evidence_digest.
CREATE OR REPLACE FUNCTION nav_level_evidence_digest_v1(
    nav_date date, nav numeric, source_nav numeric, source text,
    source_nav_kind text, currency text, nav_repair_kind text
) RETURNS char(64) LANGUAGE sql IMMUTABLE SET search_path FROM CURRENT AS $$
SELECT encode(sha256(convert_to(json_build_array(
    'nav-level-evidence-v1', $1::text, $2::text, $3::text, $4, $5, $6, $7)::text,
    'UTF8')), 'hex')::char(64)
$$;

-- Calendar maintenance is not ingestion: it never fetches, never creates a
-- provider run/attempt and may only stamp the pinned published calendar tuple
-- on already-typed rows. Scope, pins and completion are validated in the DB.
CREATE OR REPLACE FUNCTION nav_uuid_array_unique_v1(ids uuid[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
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
    -- Provider attribution is (run, provider, xid of the writing transaction);
    -- all three or none. A derived return-only update (level/provenance
    -- unchanged) records the dependency start date instead of claiming a fetch.
    source_provider text,
    source_attempt_xid xid8,
    derived_return_only boolean NOT NULL DEFAULT false,
    dependency_start_date date,
    CHECK (source_run_id IS NULL OR maintenance_run_id IS NULL),
    CHECK (num_nulls(source_run_id, source_provider, source_attempt_xid) IN (0, 3)),
    CHECK (data_changed OR calendar_changed),
    CHECK (mutation_kind = 'UPDATE' OR data_changed),
    CHECK (maintenance_run_id IS NULL
        OR (mutation_kind = 'UPDATE' AND NOT data_changed AND calendar_changed)),
    CHECK (NOT derived_return_only
        OR (mutation_kind = 'UPDATE' AND data_changed AND NOT calendar_changed)),
    CHECK (dependency_start_date IS NULL OR derived_return_only),
    FOREIGN KEY (source_run_id, instrument_id, source_provider)
        REFERENCES nav_ingestion_attempts (run_id, instrument_id, provider)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_instrument_idx
    ON fund_nav_data_revisions (instrument_id, revision_id DESC);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_window_idx
    ON fund_nav_data_revisions (instrument_id, nav_date, revision_id DESC);
CREATE INDEX IF NOT EXISTS fund_nav_data_revisions_maintenance_idx
    ON fund_nav_data_revisions (maintenance_run_id)
    WHERE maintenance_run_id IS NOT NULL;

CREATE OR REPLACE FUNCTION nav_calendar_maintenance_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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

-- Checked at COMMIT: a provider-attributed revision needs a success attempt of
-- the same run/instrument/provider persisted in the SAME transaction, finished
-- before the write, whose requested window covers the level date (or, for a
-- derived return-only update, the predecessor it depends on).
CREATE OR REPLACE FUNCTION fund_nav_revision_attribution_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    attempt record;
BEGIN
    SELECT a.status, a.commit_xid, a.finished_at, a.requested_start, a.requested_end
      INTO attempt
    FROM nav_ingestion_attempts a
    WHERE a.run_id = NEW.source_run_id AND a.instrument_id = NEW.instrument_id
      AND a.provider = NEW.source_provider;
    IF NOT FOUND
       OR attempt.status NOT IN ('success_new','success_no_new')
       OR attempt.commit_xid IS DISTINCT FROM NEW.source_attempt_xid
       OR NOT COALESCE(attempt.finished_at <= NEW.recorded_at, false)
       OR NOT COALESCE(
            NEW.nav_date BETWEEN attempt.requested_start AND attempt.requested_end
            OR (NEW.derived_return_only AND NEW.dependency_start_date
                BETWEEN attempt.requested_start AND attempt.requested_end), false) THEN
        RAISE EXCEPTION 'NAV revision lacks a same-transaction successful provider attempt';
    END IF;
    RETURN NULL;
END$$;
DROP TRIGGER IF EXISTS fund_nav_revision_attribution ON fund_nav_data_revisions;
CREATE CONSTRAINT TRIGGER fund_nav_revision_attribution
AFTER INSERT ON fund_nav_data_revisions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.source_run_id IS NOT NULL)
EXECUTE FUNCTION fund_nav_revision_attribution_v1();

-- Row evidence proves a real fetch confirmed the persisted level without a
-- fake revision (unchanged data). Server-assigned xid/instant; verified at
-- COMMIT against the persisted projection, the final head and the attempt.
CREATE TABLE IF NOT EXISTS nav_ingestion_row_evidence (
    run_id uuid NOT NULL,
    instrument_id uuid NOT NULL,
    provider text NOT NULL,
    nav_date date NOT NULL,
    level_digest char(64) NOT NULL CHECK (level_digest ~ '^[0-9a-f]{64}$'),
    observed_nav numeric NOT NULL CHECK (observed_nav > 0),
    source_nav_kind text NOT NULL CHECK (source_nav_kind IN ('adjusted','raw','unknown')),
    revision_head bigint NOT NULL CHECK (revision_head >= 0),
    commit_xid xid8 NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, instrument_id, provider, nav_date),
    FOREIGN KEY (run_id, instrument_id, provider)
        REFERENCES nav_ingestion_attempts (run_id, instrument_id, provider)
);
CREATE INDEX IF NOT EXISTS nav_ingestion_row_evidence_date_idx
    ON nav_ingestion_row_evidence (instrument_id, nav_date, recorded_at DESC);
CREATE OR REPLACE FUNCTION nav_row_evidence_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    attempt record;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV row evidence is append-only';
    END IF;
    SELECT a.status, a.commit_xid, a.requested_start, a.requested_end, r.operation
      INTO attempt
    FROM nav_ingestion_attempts a JOIN nav_ingestion_runs r USING (run_id)
    WHERE a.run_id = NEW.run_id AND a.instrument_id = NEW.instrument_id
      AND a.provider = NEW.provider;
    IF NOT FOUND OR attempt.status NOT IN ('success_new','success_no_new')
       OR attempt.commit_xid <> pg_current_xact_id()
       OR NEW.nav_date NOT BETWEEN attempt.requested_start AND attempt.requested_end THEN
        RAISE EXCEPTION 'NAV row evidence requires a same-transaction successful attempt covering its date';
    END IF;
    IF attempt.operation = 'rebase' AND NEW.source_nav_kind <> 'adjusted' THEN
        RAISE EXCEPTION 'NAV rebase row evidence must be adjusted';
    END IF;
    NEW.commit_xid := pg_current_xact_id();
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_row_evidence_guard ON nav_ingestion_row_evidence;
CREATE TRIGGER nav_row_evidence_guard
BEFORE INSERT OR UPDATE OR DELETE ON nav_ingestion_row_evidence
FOR EACH ROW EXECUTE FUNCTION nav_row_evidence_guard_v1();
CREATE OR REPLACE FUNCTION nav_row_evidence_verify_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    persisted record;
    head bigint;
BEGIN
    SELECT n.nav_date, n.nav, n.source_nav, n.source, n.source_nav_kind, n.currency,
           n.nav_repair_kind
      INTO persisted
    FROM nav_timeseries n
    WHERE n.instrument_id = NEW.instrument_id AND n.nav_date = NEW.nav_date;
    SELECT COALESCE(max(h.revision_id), 0) INTO head
    FROM fund_nav_data_heads h WHERE h.instrument_id = NEW.instrument_id;
    IF persisted.nav_date IS NULL
       OR NOT COALESCE(nav_level_evidence_digest_v1(
              persisted.nav_date, persisted.nav, persisted.source_nav, persisted.source,
              persisted.source_nav_kind, persisted.currency, persisted.nav_repair_kind)
            = NEW.level_digest, false)
       OR NOT COALESCE(persisted.source_nav = NEW.observed_nav, false)
       OR persisted.source_nav_kind IS DISTINCT FROM NEW.source_nav_kind
       OR head <> NEW.revision_head THEN
        RAISE EXCEPTION 'NAV row evidence does not match the persisted level and final head';
    END IF;
    RETURN NULL;
END$$;
DROP TRIGGER IF EXISTS nav_row_evidence_verify ON nav_ingestion_row_evidence;
CREATE CONSTRAINT TRIGGER nav_row_evidence_verify
AFTER INSERT ON nav_ingestion_row_evidence
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION nav_row_evidence_verify_v1();

-- Rebase receipt: the full-window reconciliation of one instrument, committed
-- in the same transaction as its attempt, row evidence and NAV writes. The
-- provider digest is canonical over the observations used, not raw HTTP bytes.
CREATE TABLE IF NOT EXISTS nav_rebase_receipts (
    receipt_id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    instrument_id uuid NOT NULL,
    provider text NOT NULL,
    contract_version text NOT NULL CHECK (contract_version = 'w1-tiingo-adjusted-daily-v1'),
    plan_sha256 char(64) NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    policy_hash char(64) NOT NULL,
    lifecycle_evidence_id uuid NOT NULL REFERENCES nav_instrument_policy_evidence,
    window_start date NOT NULL,
    window_end date NOT NULL,
    grid_digest char(64) NOT NULL CHECK (grid_digest ~ '^[0-9a-f]{64}$'),
    provider_snapshot_sha256 char(64) NOT NULL
        CHECK (provider_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    row_evidence_digest char(64) NOT NULL CHECK (row_evidence_digest ~ '^[0-9a-f]{64}$'),
    before_head bigint NOT NULL CHECK (before_head >= 0),
    after_head bigint NOT NULL,
    observed_levels_count integer NOT NULL CHECK (observed_levels_count >= 401),
    changed_level_rows integer NOT NULL CHECK (changed_level_rows >= 0),
    changed_return_rows integer NOT NULL CHECK (changed_return_rows >= 0),
    committed_at timestamptz NOT NULL,
    commit_xid xid8 NOT NULL,
    UNIQUE (run_id, instrument_id, provider),
    UNIQUE (plan_sha256, instrument_id),
    FOREIGN KEY (run_id, instrument_id, provider)
        REFERENCES nav_ingestion_attempts (run_id, instrument_id, provider),
    FOREIGN KEY (policy_id, policy_version) REFERENCES nav_policy_versions,
    CHECK (window_start < window_end AND before_head <= after_head)
);
CREATE OR REPLACE FUNCTION nav_rebase_receipt_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV rebase receipts are append-only';
    END IF;
    NEW.commit_xid := pg_current_xact_id();
    NEW.committed_at := clock_timestamp();
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS nav_rebase_receipt_guard ON nav_rebase_receipts;
CREATE TRIGGER nav_rebase_receipt_guard
BEFORE INSERT OR UPDATE OR DELETE ON nav_rebase_receipts
FOR EACH ROW EXECUTE FUNCTION nav_rebase_receipt_guard_v1();
-- Checked at COMMIT: governed rebase run/contract/plan, success attempt of the
-- same xid covering the window, current published unexpired policy pins,
-- latest lifecycle evidence, row evidence of the same xid for every due session
-- of the pinned calendar in the window, and the final head.
CREATE OR REPLACE FUNCTION nav_rebase_receipt_verify_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    run record;
    attempt record;
    policy record;
    latest_evidence uuid;
    head bigint;
    missing_sessions bigint;
    evidence_count bigint;
    evidence_digest text;
    grid text;
BEGIN
    SELECT r.operation, r.contract_version, r.plan_sha256 INTO run
    FROM nav_ingestion_runs r WHERE r.run_id = NEW.run_id;
    SELECT a.status, a.commit_xid, a.requested_start, a.requested_end INTO attempt
    FROM nav_ingestion_attempts a
    WHERE a.run_id = NEW.run_id AND a.instrument_id = NEW.instrument_id
      AND a.provider = NEW.provider;
    SELECT p.policy_id, p.policy_version, p.policy_hash, p.calendar_id,
           p.calendar_version, p.calendar_source
      INTO policy
    FROM nav_policy_current c JOIN nav_policy_versions p USING (policy_id, policy_version)
    WHERE c.readiness_profile = 'current_daily_nav_v1'
      AND p.published_at IS NOT NULL AND p.valid_through >= clock_timestamp();
    SELECT e.evidence_id INTO latest_evidence FROM nav_instrument_policy_evidence e
    WHERE e.instrument_id = NEW.instrument_id AND e.policy_id = NEW.policy_id
      AND e.policy_version = NEW.policy_version AND e.known_at <= clock_timestamp()
      AND e.effective_at <= clock_timestamp() AND e.recorded_at <= clock_timestamp()
    ORDER BY e.effective_at DESC, e.known_at DESC, e.recorded_at DESC, e.evidence_id DESC
    LIMIT 1;
    SELECT COALESCE(max(h.revision_id), 0) INTO head
    FROM fund_nav_data_heads h WHERE h.instrument_id = NEW.instrument_id;
    SELECT count(*) INTO missing_sessions
    FROM nav_valuation_schedules s
    WHERE s.calendar_id = policy.calendar_id AND s.calendar_version = policy.calendar_version
      AND s.session_date BETWEEN NEW.window_start AND NEW.window_end
      AND s.nav_due_at <= clock_timestamp()
      AND NOT EXISTS (SELECT 1 FROM nav_ingestion_row_evidence ev
                      WHERE ev.run_id = NEW.run_id AND ev.instrument_id = NEW.instrument_id
                        AND ev.provider = NEW.provider AND ev.nav_date = s.session_date
                        AND ev.commit_xid = NEW.commit_xid);
    SELECT '[' || COALESCE(string_agg('"' || s.session_date::text || '"', ','
                                      ORDER BY s.session_date), '') || ']'
      INTO grid
    FROM nav_valuation_schedules s
    WHERE s.calendar_id = policy.calendar_id AND s.calendar_version = policy.calendar_version
      AND s.session_date BETWEEN NEW.window_start AND NEW.window_end
      AND s.nav_due_at <= clock_timestamp();
    SELECT count(*), '[' || COALESCE(string_agg(
               '["' || ev.nav_date::text || '","' || ev.level_digest || '"]', ','
               ORDER BY ev.nav_date), '') || ']'
      INTO evidence_count, evidence_digest
    FROM nav_ingestion_row_evidence ev
    WHERE ev.run_id = NEW.run_id AND ev.instrument_id = NEW.instrument_id
      AND ev.provider = NEW.provider AND ev.commit_xid = NEW.commit_xid
      AND ev.nav_date BETWEEN NEW.window_start AND NEW.window_end;
    IF run.operation IS DISTINCT FROM 'rebase'
       OR run.contract_version IS DISTINCT FROM NEW.contract_version
       OR run.plan_sha256 IS DISTINCT FROM NEW.plan_sha256
       OR attempt.status IS DISTINCT FROM 'success_new'
          AND attempt.status IS DISTINCT FROM 'success_no_new'
       OR attempt.commit_xid IS DISTINCT FROM NEW.commit_xid
       OR NOT COALESCE(attempt.requested_start <= NEW.window_start
                       AND attempt.requested_end >= NEW.window_end, false)
       OR (policy.policy_id, policy.policy_version, policy.policy_hash)
          IS DISTINCT FROM (NEW.policy_id, NEW.policy_version, NEW.policy_hash)
       OR latest_evidence IS DISTINCT FROM NEW.lifecycle_evidence_id
       OR head <> NEW.after_head
       OR missing_sessions <> 0
       OR evidence_count <> NEW.observed_levels_count
       OR encode(sha256(convert_to(grid, 'UTF8')), 'hex') <> NEW.grid_digest
       OR encode(sha256(convert_to(evidence_digest, 'UTF8')), 'hex')
          <> NEW.row_evidence_digest THEN
        RAISE EXCEPTION 'NAV rebase receipt does not match its governed full-window evidence';
    END IF;
    RETURN NULL;
END$$;
DROP TRIGGER IF EXISTS nav_rebase_receipt_verify ON nav_rebase_receipts;
CREATE CONSTRAINT TRIGGER nav_rebase_receipt_verify
AFTER INSERT ON nav_rebase_receipts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION nav_rebase_receipt_verify_v1();

-- N1-a: append-only reexpression ledger. DETECTED opens a hold; RESOLVED closes
-- exactly one DETECTED event of the same instrument, in the same transaction as
-- a validated full-window receipt whose window covers the whole event.
CREATE TABLE IF NOT EXISTS fund_nav_reexpression_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_id uuid NOT NULL,
    event_kind text NOT NULL CHECK (event_kind IN ('DETECTED','RESOLVED')),
    first_changed_date date NOT NULL,
    last_changed_date date NOT NULL,
    source_run_id uuid NOT NULL,
    source_provider text NOT NULL,
    revision_head bigint NOT NULL CHECK (revision_head >= 0),
    recorded_at timestamptz NOT NULL,
    reason_code text NOT NULL CHECK (reason_code IN (
        'ADJUSTED_HISTORY_REEXPRESSION','FULL_WINDOW_RECONCILED')),
    resolves_event_id bigint REFERENCES fund_nav_reexpression_events (event_id),
    rebase_receipt_id uuid REFERENCES nav_rebase_receipts (receipt_id),
    CHECK (first_changed_date <= last_changed_date),
    CHECK ((event_kind = 'DETECTED' AND resolves_event_id IS NULL
            AND rebase_receipt_id IS NULL AND reason_code = 'ADJUSTED_HISTORY_REEXPRESSION')
        OR (event_kind = 'RESOLVED' AND resolves_event_id IS NOT NULL
            AND rebase_receipt_id IS NOT NULL AND reason_code = 'FULL_WINDOW_RECONCILED')),
    FOREIGN KEY (source_run_id, instrument_id, source_provider)
        REFERENCES nav_ingestion_attempts (run_id, instrument_id, provider)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE UNIQUE INDEX IF NOT EXISTS fund_nav_reexpression_events_resolves_idx
    ON fund_nav_reexpression_events (resolves_event_id)
    WHERE resolves_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS fund_nav_reexpression_events_instrument_idx
    ON fund_nav_reexpression_events (instrument_id, event_id DESC);
CREATE OR REPLACE FUNCTION fund_nav_reexpression_event_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    original record;
    receipt record;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'NAV reexpression events are append-only';
    END IF;
    PERFORM 1 FROM fund_nav_data_heads h WHERE h.instrument_id = NEW.instrument_id
    FOR UPDATE;
    SELECT COALESCE(max(h.revision_id), 0) INTO NEW.revision_head
    FROM fund_nav_data_heads h WHERE h.instrument_id = NEW.instrument_id;
    NEW.recorded_at := clock_timestamp();
    IF NEW.event_kind = 'DETECTED' THEN
        RETURN NEW;
    END IF;
    SELECT e.* INTO original FROM fund_nav_reexpression_events e
    WHERE e.event_id = NEW.resolves_event_id FOR UPDATE;
    SELECT r.* INTO receipt FROM nav_rebase_receipts r
    WHERE r.receipt_id = NEW.rebase_receipt_id;
    IF original.event_id IS NULL OR original.event_kind <> 'DETECTED'
       OR original.instrument_id <> NEW.instrument_id
       OR receipt.receipt_id IS NULL
       OR receipt.instrument_id <> NEW.instrument_id
       OR receipt.commit_xid <> pg_current_xact_id()
       OR (receipt.run_id, receipt.provider)
          IS DISTINCT FROM (NEW.source_run_id, NEW.source_provider)
       OR NEW.first_changed_date > original.first_changed_date
       OR NEW.last_changed_date < original.last_changed_date
       OR NEW.first_changed_date < receipt.window_start
       OR NEW.last_changed_date > receipt.window_end THEN
        RAISE EXCEPTION 'NAV reexpression resolution must cover one DETECTED event with a same-transaction receipt';
    END IF;
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS fund_nav_reexpression_event_guard ON fund_nav_reexpression_events;
CREATE TRIGGER fund_nav_reexpression_event_guard
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_reexpression_events
FOR EACH ROW EXECUTE FUNCTION fund_nav_reexpression_event_guard_v1();
-- Private current-active view (the old table name, for readers only): a
-- DETECTED event without a RESOLVED one. Newer detections stay active.
CREATE OR REPLACE VIEW fund_nav_reexpression_holds AS
SELECT d.event_id, d.instrument_id, d.first_changed_date, d.last_changed_date,
       d.source_run_id, d.source_provider, d.revision_head, d.reason_code,
       d.recorded_at AS detected_at
FROM fund_nav_reexpression_events d
WHERE d.event_kind = 'DETECTED'
  AND NOT EXISTS (
      SELECT 1 FROM fund_nav_reexpression_events r
      WHERE r.event_kind = 'RESOLVED' AND r.resolves_event_id = d.event_id
  );
-- The calendar tuple (calendar_id, calendar_version, calendar_source) is one
-- assertion: partial tuples and resets to NULL are rejected, and a new stamp
-- must be an attributed session of the current published policy. UPDATE
-- revisions distinguish economic data from calendar-only metadata.
CREATE OR REPLACE FUNCTION fund_nav_stamp_revision_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    affected_instrument uuid;
    affected_date date;
    new_revision bigint;
    ingestion_run uuid;
    ingestion_provider text;
    maintenance_run uuid;
    data_change boolean;
    calendar_change boolean;
    level_change boolean := true;
    derived boolean := false;
    dependency date;
    asserts_stamp boolean := false;
    maintenance record;
    lifecycle record;
BEGIN
    ingestion_run := NULLIF(current_setting('nav.ingestion_run_id', true), '')::uuid;
    ingestion_provider := NULLIF(current_setting('nav.ingestion_provider', true), '');
    maintenance_run := NULLIF(current_setting('nav.maintenance_run_id', true), '')::uuid;
    IF ingestion_run IS NOT NULL AND maintenance_run IS NOT NULL THEN
        RAISE EXCEPTION 'NAV ingestion and maintenance contexts are mutually exclusive';
    END IF;
    IF (ingestion_run IS NULL) <> (ingestion_provider IS NULL) THEN
        RAISE EXCEPTION 'NAV ingestion attribution requires both run and provider';
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
            -- Level/provenance projection vs derived-return projection.
            level_change :=
                (to_jsonb(OLD) - ARRAY['calendar_id','calendar_version','calendar_source',
                    'return_1d','return_start_date','return_source_boundary',
                    'return_uses_repaired_nav','return_semantics',
                    'return_verification_status'])
                IS DISTINCT FROM
                (to_jsonb(NEW) - ARRAY['calendar_id','calendar_version','calendar_source',
                    'return_1d','return_start_date','return_source_boundary',
                    'return_uses_repaired_nav','return_semantics',
                    'return_verification_status']);
            derived := data_change AND NOT level_change AND NOT calendar_change;
            IF derived THEN
                dependency := COALESCE(NEW.return_start_date, OLD.return_start_date);
            END IF;
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
         data_changed, calendar_changed, source_provider, source_attempt_xid,
         derived_return_only, dependency_start_date)
    VALUES (affected_instrument, affected_date, TG_OP, ingestion_run, maintenance_run,
            data_change, calendar_change, ingestion_provider,
            CASE WHEN ingestion_run IS NOT NULL THEN pg_current_xact_id() END,
            derived, dependency)
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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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
        NEW.completed_at := clock_timestamp();  -- server instant (N3)
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'NAV risk run status transition is not permitted';
END$$;
DROP TRIGGER IF EXISTS fund_nav_risk_run_guard ON fund_nav_risk_runs;
CREATE TRIGGER fund_nav_risk_run_guard
BEFORE INSERT OR UPDATE OR DELETE ON fund_nav_risk_runs
FOR EACH ROW EXECUTE FUNCTION fund_nav_risk_run_guard_v1();

CREATE OR REPLACE FUNCTION fund_nav_risk_evidence_guard_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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
        NEW.computed_at := clock_timestamp();  -- server instant (N3)
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
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    parent_state text;
    parent_run_id uuid;
BEGIN
    IF TG_TABLE_NAME = 'fund_nav_readiness_runs' THEN
        IF TG_OP = 'DELETE' OR OLD.state = 'complete' THEN
            RAISE EXCEPTION 'completed NAV readiness run is immutable';
        END IF;
        IF NEW.state = 'complete' THEN
            NEW.completed_at := clock_timestamp();  -- server instant (N3)
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

CREATE OR REPLACE FUNCTION fund_nav_readiness_pointer_stamp_v1() RETURNS trigger
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    stamp timestamptz := clock_timestamp();
    target record;
BEGIN
    IF TG_OP = 'UPDATE' AND stamp <= OLD.published_at THEN
        RAISE EXCEPTION 'publication_clock_regressed';
    END IF;
    SELECT run.state, run.completed_at, run.readiness_profile INTO target
    FROM fund_nav_readiness_runs run WHERE run.run_id = NEW.run_id;
    IF target.state IS DISTINCT FROM 'complete'
       OR target.readiness_profile IS DISTINCT FROM NEW.readiness_profile
       OR NOT COALESCE(target.completed_at <= stamp, false) THEN
        RAISE EXCEPTION 'NAV readiness pointer target is not a completed run';
    END IF;
    NEW.published_at := stamp;
    RETURN NEW;
END$$;
DROP TRIGGER IF EXISTS fund_nav_readiness_pointer_stamp ON fund_nav_readiness_current;
CREATE TRIGGER fund_nav_readiness_pointer_stamp
BEFORE INSERT OR UPDATE ON fund_nav_readiness_current
FOR EACH ROW EXECUTE FUNCTION fund_nav_readiness_pointer_stamp_v1();

-- Current-pointer semantics, not a PIT ledger: every server instant used as
-- evidence (both pointers, policy publication, risk completion, feature
-- computation, lifecycle knowledge) must be <= t; a superseded pointer, newer
-- NAV head, current hold or replaced risk can still make an older t false.
-- Instants are server clock readings inside the writing transaction, not
-- commit timestamps; readers only see committed transactions.
CREATE OR REPLACE FUNCTION fund_nav_snapshot_current_at_v1(
    subject_id uuid, selected_run_id uuid, evaluated_at timestamptz
) RETURNS boolean LANGUAGE sql STABLE AS $$
SELECT COALESCE((
    SELECT run.state='complete' AND $3 >= run.completed_at
       AND p.published_at <= $3
       AND $3 <= policy.valid_through
       AND policy.policy_hash = run.policy_hash
       AND policy.published_at <= $3
       AND active_policy.published_at <= $3
       AND published_risk.completed_at <= $3
       AND published_risk.policy_id = run.policy_id
       AND published_risk.policy_version = run.policy_version
       AND published_risk.policy_hash = run.policy_hash
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
                  AND hold.last_changed_date >= run.window_start
                  AND hold.first_changed_date <= run.window_end
            )
            AND EXISTS (
                SELECT 1 FROM fund_nav_feature_evidence feature
                WHERE feature.risk_run_id = r.risk_run_id
                  AND feature.instrument_id = r.instrument_id
                  AND feature.computed_at <= $3
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
          AND e.known_at <= $3 AND e.effective_at <= $3 AND e.recorded_at <= $3
        ORDER BY e.effective_at DESC, e.known_at DESC, e.recorded_at DESC,
                 e.evidence_id DESC
        LIMIT 1
    ) active ON true
    WHERE p.readiness_profile='current_daily_nav_v1'
      AND p.run_id=$2
    LIMIT 1
), false)
$$ SECURITY DEFINER SET search_path FROM CURRENT;

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

-- Access profile light_app_runtime_v1 (N6). Only local W1 grants: PUBLIC has
-- nothing on W1 functions (default EXECUTE revoked, helpers and triggers
-- included); app_runtime gets SELECT on exactly seven read models and EXECUTE
-- on the snapshot, without grant option. Roles and memberships are never
-- created or altered here; a missing role is reported by the operator.
REVOKE ALL ON FUNCTION nav_policy_freeze_v1(), nav_policy_pointer_stamp_v1(),
    nav_instrument_evidence_append_only_v1(), nav_policy_publication_receipt_guard_v1(),
    nav_policy_evidence_digest_v1(text,text),
    nav_ingestion_run_guard_v1(),
    nav_ingestion_attempt_guard_v1(),
    nav_level_evidence_digest_v1(date,numeric,numeric,text,text,text,text),
    nav_uuid_array_unique_v1(uuid[]), nav_calendar_maintenance_guard_v1(),
    fund_nav_revision_append_only_v1(), fund_nav_revision_attribution_v1(),
    nav_row_evidence_guard_v1(), nav_row_evidence_verify_v1(),
    nav_rebase_receipt_guard_v1(), nav_rebase_receipt_verify_v1(),
    fund_nav_reexpression_event_guard_v1(), fund_nav_stamp_revision_v1(),
    fund_nav_risk_run_guard_v1(), fund_nav_risk_evidence_guard_v1(),
    fund_nav_readiness_freeze_v1(), fund_nav_readiness_pointer_stamp_v1(),
    fund_nav_snapshot_current_at_v1(uuid,uuid,timestamptz)
    FROM PUBLIC;
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
