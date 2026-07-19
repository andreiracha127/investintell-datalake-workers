-- Contrato compartilhado de proveniência e reconciliação das fontes SEC.
-- Não depende de extensões: UUIDs são gerados pelo chamador Python.

CREATE TABLE IF NOT EXISTS sec_ingestion_runs (
    run_id uuid PRIMARY KEY,
    source_family text NOT NULL CHECK (source_family <> ''),
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    parser_version text NOT NULL CHECK (parser_version <> ''),
    source_quarter text NOT NULL CHECK (source_quarter ~ '^[0-9]{4}Q[1-4]$'),
    package_relative_path text NOT NULL CHECK (
        package_relative_path <> ''
        AND package_relative_path !~ '(^|/)[.][.]?(/|$)'
        AND position(chr(92) in package_relative_path) = 0
    ),
    current_state text NOT NULL DEFAULT 'discovered' CHECK (current_state IN (
        'discovered', 'loading', 'raw_validated', 'derived_building',
        'derived_validated', 'published', 'failed'
    )),
    raw_validated_at timestamptz,
    published_at timestamptz,
    failure_code text,
    failure_detail text,
    retry_state text CHECK (retry_state IS NULL OR retry_state IN (
        'discovered', 'loading', 'raw_validated', 'derived_building', 'derived_validated'
    )),
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_family, package_sha256, parser_version),
    CHECK ((raw_validated_at IS NULL) OR current_state <> 'discovered'),
    CHECK ((published_at IS NULL) OR current_state = 'published'),
    CHECK (NOT (current_state = 'published' AND published_at IS NULL)),
    CHECK (NOT (current_state = 'failed' AND retry_state IS NULL))
);

CREATE TABLE IF NOT EXISTS sec_source_packages (
    package_id uuid PRIMARY KEY,
    source_family text NOT NULL CHECK (source_family <> ''),
    source_quarter text NOT NULL CHECK (source_quarter ~ '^[0-9]{4}Q[1-4]$'),
    package_relative_path text NOT NULL CHECK (
        package_relative_path <> ''
        AND package_relative_path !~ '(^|/)[.][.]?(/|$)'
        AND position(chr(92) in package_relative_path) = 0
    ),
    package_sha256 char(64) CHECK (package_sha256 IS NULL OR package_sha256 ~ '^[0-9a-f]{64}$'),
    metadata_sha256 char(64) CHECK (metadata_sha256 IS NULL OR metadata_sha256 ~ '^[0-9a-f]{64}$'),
    readme_sha256 char(64) CHECK (readme_sha256 IS NULL OR readme_sha256 ~ '^[0-9a-f]{64}$'),
    package_state text NOT NULL CHECK (package_state IN (
        'discovered', 'loaded', 'duplicate', 'unsupported', 'quarantined', 'failed'
    )),
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    reason text,
    run_id uuid REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    duplicate_of_package_id uuid REFERENCES sec_source_packages(package_id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_family, package_relative_path),
    CHECK (package_state IN ('discovered', 'loaded') OR NULLIF(reason, '') IS NOT NULL),
    CHECK ((package_state = 'duplicate') = (duplicate_of_package_id IS NOT NULL)),
    CHECK (duplicate_of_package_id IS NULL OR duplicate_of_package_id <> package_id),
    CHECK (package_state <> 'loaded' OR run_id IS NOT NULL)
);

ALTER TABLE sec_source_packages
    ADD COLUMN IF NOT EXISTS retry_count integer NOT NULL DEFAULT 0;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_packages_retry_count_nonnegative_ck') THEN
        ALTER TABLE sec_source_packages
            ADD CONSTRAINT sec_packages_retry_count_nonnegative_ck CHECK (retry_count >= 0);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS sec_source_files (
    source_file_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    relative_path text NOT NULL CHECK (
        relative_path <> ''
        AND relative_path !~ '(^|/)[.][.]?(/|$)'
        AND position(chr(92) in relative_path) = 0
    ),
    sha256 char(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    byte_size bigint NOT NULL CHECK (byte_size >= 0),
    schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    readme_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    expected_count bigint NOT NULL DEFAULT 0 CHECK (expected_count >= 0),
    data_count bigint NOT NULL DEFAULT 0 CHECK (data_count >= 0),
    lexical_count bigint NOT NULL DEFAULT 0 CHECK (lexical_count >= 0),
    typed_success_count bigint NOT NULL DEFAULT 0 CHECK (typed_success_count >= 0),
    quarantine_count bigint NOT NULL DEFAULT 0 CHECK (quarantine_count >= 0),
    reject_count bigint NOT NULL DEFAULT 0 CHECK (reject_count >= 0),
    state text NOT NULL DEFAULT 'discovered' CHECK (state IN ('discovered', 'loading', 'accounted', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, relative_path),
    UNIQUE (run_id, source_file_id)
);

CREATE TABLE IF NOT EXISTS sec_source_package_transitions (
    package_transition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES sec_source_packages(package_id) ON DELETE RESTRICT,
    from_state text,
    to_state text NOT NULL,
    retry_count integer NOT NULL CHECK (retry_count >= 0),
    terminal_reason text,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

-- A instalação pode retomar um banco descartável que recebeu a primeira
-- revisão desta Fase 1, onde este campo usava o nome menos preciso source_count.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sec_source_files' AND column_name = 'source_count'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'sec_source_files' AND column_name = 'data_count'
    ) THEN
        ALTER TABLE sec_source_files RENAME COLUMN source_count TO data_count;
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS sec_table_reconciliations (
    reconciliation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL,
    source_file_id uuid NOT NULL,
    table_name text NOT NULL CHECK (table_name <> ''),
    expected_count bigint NOT NULL DEFAULT 0 CHECK (expected_count >= 0),
    source_count bigint NOT NULL DEFAULT 0 CHECK (source_count >= 0),
    lexical_count bigint NOT NULL DEFAULT 0 CHECK (lexical_count >= 0),
    typed_success_count bigint NOT NULL DEFAULT 0 CHECK (typed_success_count >= 0),
    quarantine_count bigint NOT NULL DEFAULT 0 CHECK (quarantine_count >= 0),
    reject_count bigint NOT NULL DEFAULT 0 CHECK (reject_count >= 0),
    state text NOT NULL DEFAULT 'discovered' CHECK (state IN ('discovered', 'loading', 'accounted', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (run_id, source_file_id)
        REFERENCES sec_source_files(run_id, source_file_id) ON DELETE RESTRICT,
    UNIQUE (source_file_id, table_name)
);

CREATE TABLE IF NOT EXISTS sec_row_issues (
    issue_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file_id uuid NOT NULL REFERENCES sec_source_files(source_file_id) ON DELETE RESTRICT,
    source_row_number bigint NOT NULL CHECK (source_row_number >= 0),
    issue_sequence smallint NOT NULL DEFAULT 1 CHECK (issue_sequence > 0),
    table_name text,
    column_name text,
    raw_lexical_value text,
    typed_error_code text NOT NULL CHECK (typed_error_code <> ''),
    typed_error_detail text,
    status text NOT NULL CHECK (status IN ('quarantined', 'rejected', 'resolved')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_file_id, source_row_number, issue_sequence)
);

CREATE TABLE IF NOT EXISTS sec_run_transitions (
    transition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    from_state text,
    to_state text NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('created', 'transition', 'raw_validated', 'failed', 'retry')),
    detail text,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

-- Governed Phase-4 evidence deliberately reuses the two append-only transition
-- ledgers.  Lifecycle events retain their original rows and carry no canonical
-- governance data; free-text detail/reason remains diagnostic only.
ALTER TABLE sec_run_transitions
    ADD COLUMN IF NOT EXISTS supervisor_run_id uuid,
    ADD COLUMN IF NOT EXISTS authorization_fingerprint char(64),
    ADD COLUMN IF NOT EXISTS governed_package_sha256 char(64),
    ADD COLUMN IF NOT EXISTS commit_outcome text;

ALTER TABLE sec_source_package_transitions
    ADD COLUMN IF NOT EXISTS event_kind text NOT NULL DEFAULT 'lifecycle',
    ADD COLUMN IF NOT EXISTS ingestion_run_id uuid REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS supervisor_run_id uuid,
    ADD COLUMN IF NOT EXISTS authorization_fingerprint char(64),
    ADD COLUMN IF NOT EXISTS governed_package_sha256 char(64),
    ADD COLUMN IF NOT EXISTS reconciliation_sha256 char(64),
    ADD COLUMN IF NOT EXISTS certificate_id uuid,
    ADD COLUMN IF NOT EXISTS certificate_sha256 char(64);

DO $$
BEGIN
    -- The original anonymous check permits only lifecycle events.  Replacing it
    -- by name keeps a disposable database from an earlier installer revision
    -- resumable while this pre-install DDL remains idempotent.
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_run_transitions_event_type_check') THEN
        ALTER TABLE public.sec_run_transitions DROP CONSTRAINT sec_run_transitions_event_type_check;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_run_transition_event_type_ck') THEN
        ALTER TABLE sec_run_transitions ADD CONSTRAINT sec_run_transition_event_type_ck CHECK (
            event_type IN (
                'created', 'transition', 'raw_validated', 'failed', 'retry',
                'commit_outcome', 'commit_outcome_resolution'
            )
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_run_transition_governance_fields_ck') THEN
        ALTER TABLE sec_run_transitions ADD CONSTRAINT sec_run_transition_governance_fields_ck CHECK (
            (
                event_type IN ('created', 'transition', 'raw_validated', 'failed', 'retry')
                AND supervisor_run_id IS NULL AND authorization_fingerprint IS NULL
                AND governed_package_sha256 IS NULL AND commit_outcome IS NULL
            )
            OR (
                event_type = 'commit_outcome'
                AND supervisor_run_id IS NOT NULL
                AND authorization_fingerprint ~ '^[0-9a-f]{64}$'
                AND governed_package_sha256 ~ '^[0-9a-f]{64}$'
                AND commit_outcome IN ('committed', 'rolled_back', 'ambiguous')
            )
            OR (
                event_type = 'commit_outcome_resolution'
                AND supervisor_run_id IS NOT NULL
                AND authorization_fingerprint ~ '^[0-9a-f]{64}$'
                AND governed_package_sha256 ~ '^[0-9a-f]{64}$'
                AND commit_outcome IN ('committed', 'rolled_back')
            )
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_package_transition_event_kind_ck') THEN
        ALTER TABLE sec_source_package_transitions ADD CONSTRAINT sec_package_transition_event_kind_ck CHECK (
            event_kind IN ('lifecycle', 'canary_promoted')
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_package_transition_governance_fields_ck') THEN
        ALTER TABLE sec_source_package_transitions ADD CONSTRAINT sec_package_transition_governance_fields_ck CHECK (
            (
                event_kind = 'lifecycle'
                AND ingestion_run_id IS NULL AND supervisor_run_id IS NULL
                AND authorization_fingerprint IS NULL AND governed_package_sha256 IS NULL
                AND reconciliation_sha256 IS NULL AND certificate_id IS NULL
                AND certificate_sha256 IS NULL
            )
            OR (
                event_kind = 'canary_promoted'
                AND ingestion_run_id IS NOT NULL AND supervisor_run_id IS NOT NULL
                AND authorization_fingerprint ~ '^[0-9a-f]{64}$'
                AND governed_package_sha256 ~ '^[0-9a-f]{64}$'
                AND reconciliation_sha256 ~ '^[0-9a-f]{64}$'
                AND certificate_id IS NOT NULL AND certificate_sha256 ~ '^[0-9a-f]{64}$'
            )
        );
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS sec_validated_raw_visibility (
    run_id uuid PRIMARY KEY REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    raw_validated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sec_raw_validation_tokens (
    run_id uuid PRIMARY KEY REFERENCES sec_ingestion_runs(run_id) ON DELETE CASCADE,
    backend_pid integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON TABLE sec_raw_validation_tokens FROM PUBLIC;

CREATE OR REPLACE VIEW sec_validated_raw_runs AS
SELECT r.run_id, r.source_family, r.package_sha256, r.parser_version,
       r.source_quarter, r.package_relative_path, r.raw_validated_at
FROM sec_ingestion_runs AS r
JOIN sec_validated_raw_visibility AS v ON v.run_id = r.run_id
WHERE r.raw_validated_at IS NOT NULL;

-- Restrições nomeadas permitem reforçar instalações da primeira revisão sem
-- depender dos nomes automáticos de CHECK já existentes.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_runs_path_normalized_ck') THEN
        ALTER TABLE sec_ingestion_runs ADD CONSTRAINT sec_runs_path_normalized_ck CHECK (
            package_relative_path <> '' AND left(package_relative_path, 1) <> '/'
            AND package_relative_path !~ '^[A-Za-z]:'
            AND package_relative_path !~ '(^|/)[.][.]?(/|$)'
            AND position(chr(92) in package_relative_path) = 0
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_packages_path_normalized_ck') THEN
        ALTER TABLE sec_source_packages ADD CONSTRAINT sec_packages_path_normalized_ck CHECK (
            package_relative_path <> '' AND left(package_relative_path, 1) <> '/'
            AND package_relative_path !~ '^[A-Za-z]:'
            AND package_relative_path !~ '(^|/)[.][.]?(/|$)'
            AND position(chr(92) in package_relative_path) = 0
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sec_files_path_normalized_ck') THEN
        ALTER TABLE sec_source_files ADD CONSTRAINT sec_files_path_normalized_ck CHECK (
            relative_path <> '' AND left(relative_path, 1) <> '/'
            AND relative_path !~ '^[A-Za-z]:'
            AND relative_path !~ '(^|/)[.][.]?(/|$)'
            AND position(chr(92) in relative_path) = 0
        );
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS sec_ingestion_runs_state_idx
    ON sec_ingestion_runs (source_family, current_state, created_at DESC);
CREATE INDEX IF NOT EXISTS sec_source_packages_family_quarter_idx
    ON sec_source_packages (source_family, source_quarter, package_state);
CREATE INDEX IF NOT EXISTS sec_source_packages_hash_idx
    ON sec_source_packages (source_family, package_sha256) WHERE package_sha256 IS NOT NULL;
CREATE INDEX IF NOT EXISTS sec_source_files_run_idx ON sec_source_files (run_id);
CREATE INDEX IF NOT EXISTS sec_source_package_transitions_package_idx
    ON sec_source_package_transitions (package_id, occurred_at);
CREATE INDEX IF NOT EXISTS sec_table_reconciliations_run_idx ON sec_table_reconciliations (run_id, source_file_id);
CREATE INDEX IF NOT EXISTS sec_row_issues_file_row_idx ON sec_row_issues (source_file_id, source_row_number);
CREATE INDEX IF NOT EXISTS sec_run_transitions_run_idx ON sec_run_transitions (run_id, occurred_at);
CREATE UNIQUE INDEX IF NOT EXISTS sec_run_transitions_definitive_outcome_uq
    ON sec_run_transitions (run_id, authorization_fingerprint)
    WHERE event_type IN ('commit_outcome', 'commit_outcome_resolution')
      AND commit_outcome IN ('committed', 'rolled_back');
CREATE UNIQUE INDEX IF NOT EXISTS sec_run_transitions_ambiguous_outcome_uq
    ON sec_run_transitions (run_id, authorization_fingerprint)
    WHERE event_type = 'commit_outcome' AND commit_outcome = 'ambiguous';
CREATE UNIQUE INDEX IF NOT EXISTS sec_source_package_transitions_canary_promotion_uq
    ON sec_source_package_transitions (package_id)
    WHERE event_kind = 'canary_promoted';
CREATE UNIQUE INDEX IF NOT EXISTS sec_source_package_transitions_certificate_uq
    ON sec_source_package_transitions (certificate_id)
    WHERE event_kind = 'canary_promoted';

CREATE OR REPLACE FUNCTION sec_raw_run_reconciles(target_run_id uuid)
RETURNS boolean
LANGUAGE sql
VOLATILE
AS $$
    SELECT EXISTS (SELECT 1 FROM sec_source_files WHERE run_id = target_run_id)
       AND NOT EXISTS (SELECT 1 FROM sec_source_files WHERE run_id = target_run_id AND state <> 'accounted')
       AND NOT EXISTS (SELECT 1 FROM sec_table_reconciliations WHERE run_id = target_run_id AND state <> 'accounted')
       AND NOT EXISTS (
           SELECT 1 FROM sec_source_files AS f
           LEFT JOIN LATERAL (
               SELECT count(DISTINCT source_row_number) FILTER (WHERE status = 'quarantined') AS quarantined,
                      count(DISTINCT source_row_number) FILTER (WHERE status = 'rejected') AS rejected
               FROM sec_row_issues WHERE source_file_id = f.source_file_id
           ) AS i ON TRUE
           WHERE f.run_id = target_run_id
             AND (f.quarantine_count <> i.quarantined OR f.reject_count <> i.rejected)
       )
       AND NOT EXISTS (
           SELECT 1 FROM sec_table_reconciliations AS t
           LEFT JOIN LATERAL (
               SELECT count(DISTINCT source_row_number) FILTER (WHERE status = 'quarantined') AS quarantined,
                      count(DISTINCT source_row_number) FILTER (WHERE status = 'rejected') AS rejected
               FROM sec_row_issues
               WHERE source_file_id = t.source_file_id AND table_name = t.table_name
           ) AS i ON TRUE
           WHERE t.run_id = target_run_id
             AND (t.quarantine_count <> i.quarantined OR t.reject_count <> i.rejected)
       )
       AND NOT EXISTS (
           SELECT 1 FROM sec_source_files
           WHERE run_id = target_run_id AND (
               expected_count <> data_count OR data_count <> lexical_count
               OR lexical_count <> typed_success_count + quarantine_count + reject_count
           )
           UNION ALL
           SELECT 1 FROM sec_table_reconciliations
           WHERE run_id = target_run_id AND (
               expected_count <> source_count OR source_count <> lexical_count
               OR lexical_count <> typed_success_count + quarantine_count + reject_count
           )
       );
$$;

CREATE OR REPLACE FUNCTION sec_lock_manifest_run_lineage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT run_id, raw_validated_at FROM sec_ingestion_runs
        WHERE run_id = ANY (
            CASE WHEN TG_OP = 'INSERT' THEN ARRAY[NEW.run_id]
                 WHEN TG_OP = 'DELETE' THEN ARRAY[OLD.run_id]
                 ELSE ARRAY[OLD.run_id, NEW.run_id] END
        )
        ORDER BY run_id FOR UPDATE
    LOOP
        IF item.raw_validated_at IS NOT NULL THEN
            RAISE EXCEPTION 'raw accounting for run % is immutable after validation', item.run_id;
        END IF;
    END LOOP;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE OR REPLACE FUNCTION sec_lock_issue_run_lineage()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT r.run_id, r.raw_validated_at
        FROM sec_source_files AS f
        JOIN sec_ingestion_runs AS r ON r.run_id = f.run_id
        WHERE f.source_file_id = ANY (
            CASE WHEN TG_OP = 'INSERT' THEN ARRAY[NEW.source_file_id]
                 WHEN TG_OP = 'DELETE' THEN ARRAY[OLD.source_file_id]
                 ELSE ARRAY[OLD.source_file_id, NEW.source_file_id] END
        )
        ORDER BY r.run_id FOR UPDATE OF r
    LOOP
        IF item.raw_validated_at IS NOT NULL THEN
            RAISE EXCEPTION 'row issues for run % are immutable after raw validation', item.run_id;
        END IF;
    END LOOP;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE OR REPLACE FUNCTION sec_check_issue_active_disposition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE existing_status text;
        issue_run_id uuid;
BEGIN
    IF NEW.status NOT IN ('quarantined', 'rejected') THEN
        RETURN NEW;
    END IF;
    -- Serialize dispositions once per ingestion run, not once per physical
    -- row.  The former exhausted max_locks_per_transaction on large COPY
    -- batches; the run-scoped lock is bounded to one lock per transaction
    -- while preserving the concurrent opposite-status invariant.
    SELECT run_id INTO issue_run_id
    FROM sec_source_files
    WHERE source_file_id = NEW.source_file_id;
    IF issue_run_id IS NULL THEN
        RAISE EXCEPTION 'unknown source file %', NEW.source_file_id;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('sec:issues:' || issue_run_id::text, 0));
    SELECT status INTO existing_status FROM sec_row_issues
    WHERE source_file_id = NEW.source_file_id
      AND source_row_number = NEW.source_row_number
      AND status IN ('quarantined', 'rejected')
    LIMIT 1 FOR UPDATE;
    IF existing_status IS NOT NULL AND existing_status <> NEW.status THEN
        RAISE EXCEPTION 'source row % in file % cannot be both quarantined and rejected',
            NEW.source_row_number, NEW.source_file_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION sec_raw_validation_token_present(target_run_id uuid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.sec_raw_validation_tokens
        WHERE run_id = target_run_id AND backend_pid = pg_backend_pid()
    );
$$;

CREATE OR REPLACE FUNCTION sec_run_lifecycle_guard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.run_id IS DISTINCT FROM OLD.run_id
       OR (NEW.source_family, NEW.package_sha256, NEW.parser_version, NEW.source_quarter, NEW.package_relative_path)
       IS DISTINCT FROM
       (OLD.source_family, OLD.package_sha256, OLD.parser_version, OLD.source_quarter, OLD.package_relative_path) THEN
        RAISE EXCEPTION 'run identity is immutable';
    END IF;
    IF OLD.current_state = 'published' THEN
        RAISE EXCEPTION 'published run % is terminal and immutable', OLD.run_id;
    END IF;
    IF OLD.raw_validated_at IS NOT NULL AND NEW.raw_validated_at IS DISTINCT FROM OLD.raw_validated_at THEN
        RAISE EXCEPTION 'raw validation for run % is irreversible', OLD.run_id;
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'publication timestamp is immutable';
    END IF;
    IF NEW.current_state = OLD.current_state THEN
        RAISE EXCEPTION 'run updates must use an allowed lifecycle transition';
    END IF;
    IF NEW.current_state = 'failed' THEN
        IF OLD.current_state = 'failed' OR NEW.retry_state IS DISTINCT FROM OLD.current_state
           OR NEW.failure_code IS NULL OR NEW.raw_validated_at IS DISTINCT FROM OLD.raw_validated_at
           OR NEW.published_at IS DISTINCT FROM OLD.published_at THEN
            RAISE EXCEPTION 'invalid failed lifecycle transition';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.current_state = 'failed' THEN
        IF NEW.current_state IS DISTINCT FROM OLD.retry_state OR NEW.retry_state IS NOT NULL
           OR NEW.failure_code IS NOT NULL OR NEW.failure_detail IS NOT NULL
           OR NEW.retry_count <> OLD.retry_count + 1
           OR NEW.raw_validated_at IS DISTINCT FROM OLD.raw_validated_at
           OR NEW.published_at IS DISTINCT FROM OLD.published_at THEN
            RAISE EXCEPTION 'invalid retry lifecycle transition';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.retry_state IS NOT NULL OR NEW.failure_code IS NOT NULL OR NEW.failure_detail IS NOT NULL
       OR NEW.retry_count <> OLD.retry_count THEN
        RAISE EXCEPTION 'normal lifecycle transitions cannot alter failure metadata';
    END IF;
    IF OLD.current_state = 'discovered' AND NEW.current_state = 'loading'
       AND NEW.raw_validated_at IS NULL AND NEW.published_at IS NULL THEN RETURN NEW; END IF;
    IF OLD.current_state = 'loading' AND NEW.current_state = 'raw_validated'
       AND OLD.raw_validated_at IS NULL AND NEW.raw_validated_at IS NOT NULL
       AND NEW.published_at IS NULL
       AND sec_raw_validation_token_present(OLD.run_id) THEN RETURN NEW; END IF;
    IF OLD.current_state = 'raw_validated' AND NEW.current_state = 'derived_building'
       AND NEW.raw_validated_at = OLD.raw_validated_at AND NEW.published_at IS NULL THEN RETURN NEW; END IF;
    IF OLD.current_state = 'derived_building' AND NEW.current_state = 'derived_validated'
       AND NEW.raw_validated_at = OLD.raw_validated_at AND NEW.published_at IS NULL THEN RETURN NEW; END IF;
    IF OLD.current_state = 'derived_validated' AND NEW.current_state = 'published'
       AND NEW.raw_validated_at = OLD.raw_validated_at AND OLD.published_at IS NULL
       AND NEW.published_at IS NOT NULL THEN RETURN NEW; END IF;
    RAISE EXCEPTION 'invalid lifecycle transition % -> %', OLD.current_state, NEW.current_state;
END;
$$;

CREATE OR REPLACE FUNCTION sec_validate_raw_run(target_run_id uuid, audit_detail text DEFAULT NULL)
RETURNS timestamptz
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE validated_at timestamptz; source_family_value text; family_reconciles boolean; family_label text;
BEGIN
    SELECT source_family INTO source_family_value
    FROM sec_ingestion_runs WHERE run_id = target_run_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown SEC run %', target_run_id; END IF;
    IF (SELECT current_state FROM sec_ingestion_runs WHERE run_id = target_run_id) <> 'loading' THEN
        RAISE EXCEPTION 'raw validation requires loading state';
    END IF;
    -- VOLATILE + parent row lock intentionally obtains current child visibility
    -- after any writer that held the same lineage lock has committed.
    IF NOT sec_raw_run_reconciles(target_run_id) THEN
        RAISE EXCEPTION 'raw reconciliation failed for run %', target_run_id;
    END IF;
    -- Family overlays remain separate DDL units.  Their presence activates a
    -- fail-closed hook without letting a later reinstall of this shared schema
    -- overwrite or bypass a family-specific validator.
    IF source_family_value = 'nport' THEN
        IF to_regprocedure('public.nport_raw_run_reconciles(uuid)') IS NULL THEN
            RAISE EXCEPTION 'required N-PORT raw reconciler is absent';
        END IF;
        EXECUTE 'SELECT public.nport_raw_run_reconciles($1)' INTO family_reconciles USING target_run_id;
    ELSIF source_family_value = 'ncen' THEN
        IF to_regprocedure('public.ncen_raw_run_reconciles(uuid)') IS NULL THEN
            RAISE EXCEPTION 'required NCEN raw reconciler is absent';
        END IF;
        EXECUTE 'SELECT public.ncen_raw_run_reconciles($1)' INTO family_reconciles USING target_run_id;
    ELSIF source_family_value = 'rr1' THEN
        IF to_regprocedure('public.rr1_raw_run_reconciles(uuid)') IS NULL THEN
            RAISE EXCEPTION 'required RR1 raw reconciler is absent';
        END IF;
        EXECUTE 'SELECT public.rr1_raw_run_reconciles($1)' INTO family_reconciles USING target_run_id;
    ELSE
        family_reconciles := true;
    END IF;
    IF family_reconciles IS NOT TRUE THEN
        family_label := CASE source_family_value WHEN 'nport' THEN 'N-PORT' ELSE upper(source_family_value) END;
        RAISE EXCEPTION '% raw validation failed for run %', family_label, target_run_id;
    END IF;
    INSERT INTO sec_raw_validation_tokens (run_id, backend_pid)
    VALUES (target_run_id, pg_backend_pid());
    PERFORM set_config('sec.lifecycle_detail', COALESCE(audit_detail, 'raw reconciliation exact'), true);
    UPDATE sec_ingestion_runs
    SET current_state = 'raw_validated', raw_validated_at = now(), updated_at = now()
    WHERE run_id = target_run_id AND current_state = 'loading'
    RETURNING raw_validated_at INTO validated_at;
    DELETE FROM sec_raw_validation_tokens WHERE run_id = target_run_id AND backend_pid = pg_backend_pid();
    IF validated_at IS NULL THEN RAISE EXCEPTION 'SEC run changed during raw validation'; END IF;
    RETURN validated_at;
END;
$$;

CREATE OR REPLACE FUNCTION sec_record_commit_outcome(
    target_run_id uuid,
    target_supervisor_run_id uuid,
    target_authorization_fingerprint character(64),
    target_package_sha256 character(64),
    target_outcome text
)
RETURNS TABLE (
    transition_id bigint,
    event_type text,
    run_id uuid,
    supervisor_run_id uuid,
    authorization_fingerprint character(64),
    package_sha256 character(64),
    commit_outcome text
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    persisted_package_sha256 character(64);
    existing record;
BEGIN
    IF target_outcome NOT IN ('committed', 'rolled_back', 'ambiguous') THEN
        RAISE EXCEPTION 'invalid governed commit outcome %', target_outcome;
    END IF;
    SELECT ingestion_run.package_sha256 INTO persisted_package_sha256
    FROM sec_ingestion_runs AS ingestion_run
    WHERE ingestion_run.run_id = target_run_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown SEC run %', target_run_id; END IF;
    IF persisted_package_sha256 IS DISTINCT FROM target_package_sha256 THEN
        RAISE EXCEPTION 'governed package hash does not match run %', target_run_id;
    END IF;

    SELECT run_transition.transition_id, run_transition.event_type,
           run_transition.supervisor_run_id, run_transition.governed_package_sha256,
           run_transition.commit_outcome
    INTO existing
    FROM sec_run_transitions AS run_transition
    WHERE run_transition.run_id = target_run_id
      AND run_transition.authorization_fingerprint = target_authorization_fingerprint
      AND run_transition.commit_outcome IN ('committed', 'rolled_back')
    ORDER BY run_transition.transition_id DESC
    LIMIT 1 FOR UPDATE;
    IF FOUND THEN
        IF existing.supervisor_run_id IS DISTINCT FROM target_supervisor_run_id
           OR existing.governed_package_sha256 IS DISTINCT FROM target_package_sha256
           OR existing.commit_outcome IS DISTINCT FROM target_outcome THEN
            RAISE EXCEPTION 'conflicting definitive governed commit outcome for run %', target_run_id;
        END IF;
        RETURN QUERY SELECT existing.transition_id, existing.event_type, target_run_id,
            existing.supervisor_run_id, target_authorization_fingerprint,
            existing.governed_package_sha256, existing.commit_outcome;
        RETURN;
    END IF;

    SELECT run_transition.transition_id, run_transition.event_type,
           run_transition.supervisor_run_id, run_transition.governed_package_sha256,
           run_transition.commit_outcome
    INTO existing
    FROM sec_run_transitions AS run_transition
    WHERE run_transition.run_id = target_run_id
      AND run_transition.authorization_fingerprint = target_authorization_fingerprint
      AND run_transition.commit_outcome = 'ambiguous'
    LIMIT 1 FOR UPDATE;
    IF FOUND THEN
        IF existing.supervisor_run_id IS DISTINCT FROM target_supervisor_run_id
           OR existing.governed_package_sha256 IS DISTINCT FROM target_package_sha256 THEN
            RAISE EXCEPTION 'conflicting ambiguous governed lineage for run %', target_run_id;
        END IF;
        IF target_outcome = 'ambiguous' THEN
            RETURN QUERY SELECT existing.transition_id, existing.event_type, target_run_id,
                existing.supervisor_run_id, target_authorization_fingerprint,
                existing.governed_package_sha256, existing.commit_outcome;
            RETURN;
        END IF;
        INSERT INTO sec_run_transitions (
            run_id, from_state, to_state, event_type, supervisor_run_id,
            authorization_fingerprint, governed_package_sha256, commit_outcome
        ) VALUES (
            target_run_id, NULL, 'governed_resolution', 'commit_outcome_resolution',
            target_supervisor_run_id, target_authorization_fingerprint,
            target_package_sha256, target_outcome
        )
        RETURNING sec_run_transitions.transition_id, sec_run_transitions.event_type,
            sec_run_transitions.run_id, sec_run_transitions.supervisor_run_id,
            sec_run_transitions.authorization_fingerprint,
            sec_run_transitions.governed_package_sha256, sec_run_transitions.commit_outcome
        INTO transition_id, event_type, run_id, supervisor_run_id, authorization_fingerprint,
            package_sha256, commit_outcome;
        RETURN NEXT;
        RETURN;
    END IF;

    INSERT INTO sec_run_transitions (
        run_id, from_state, to_state, event_type, supervisor_run_id,
        authorization_fingerprint, governed_package_sha256, commit_outcome
    ) VALUES (
        target_run_id, NULL, 'governed_outcome', 'commit_outcome',
        target_supervisor_run_id, target_authorization_fingerprint,
        target_package_sha256, target_outcome
    )
    RETURNING sec_run_transitions.transition_id, sec_run_transitions.event_type,
        sec_run_transitions.run_id, sec_run_transitions.supervisor_run_id,
        sec_run_transitions.authorization_fingerprint,
        sec_run_transitions.governed_package_sha256, sec_run_transitions.commit_outcome
    INTO transition_id, event_type, run_id, supervisor_run_id, authorization_fingerprint,
        package_sha256, commit_outcome;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION sec_promote_certified_canary_package(
    target_package_id uuid,
    target_ingestion_run_id uuid,
    target_supervisor_run_id uuid,
    target_authorization_fingerprint character(64),
    target_package_sha256 character(64),
    target_reconciliation_sha256 character(64),
    target_certificate_id uuid,
    target_certificate_sha256 character(64)
)
RETURNS TABLE (
    package_transition_id bigint,
    package_id uuid,
    ingestion_run_id uuid,
    certificate_id uuid,
    reconciliation_sha256 character(64)
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    package_row record;
    run_row record;
    outcome_row record;
    existing record;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('sec:certificate:' || target_certificate_id::text, 0));
    SELECT source_package.package_id, source_package.run_id, source_package.package_sha256,
           source_package.package_state, source_package.retry_count
    INTO package_row
    FROM sec_source_packages AS source_package
    WHERE source_package.package_id = target_package_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown SEC package %', target_package_id; END IF;
    IF package_row.run_id IS DISTINCT FROM target_ingestion_run_id
       OR package_row.package_sha256 IS DISTINCT FROM target_package_sha256
       OR package_row.package_state <> 'loaded' THEN
        RAISE EXCEPTION 'package/run/hash lineage is not promotable';
    END IF;
    SELECT ingestion_run.run_id, ingestion_run.package_sha256, ingestion_run.raw_validated_at
    INTO run_row
    FROM sec_ingestion_runs AS ingestion_run
    WHERE ingestion_run.run_id = target_ingestion_run_id FOR UPDATE;
    IF NOT FOUND OR run_row.package_sha256 IS DISTINCT FROM target_package_sha256
       OR run_row.raw_validated_at IS NULL
       OR NOT EXISTS (
           SELECT 1 FROM sec_validated_raw_visibility AS raw_visibility
           WHERE raw_visibility.run_id = target_ingestion_run_id
       ) THEN
        RAISE EXCEPTION 'promotion requires a raw-validated ingestion run';
    END IF;
    SELECT run_transition.supervisor_run_id, run_transition.governed_package_sha256,
           run_transition.commit_outcome
    INTO outcome_row
    FROM sec_run_transitions AS run_transition
    WHERE run_transition.run_id = target_ingestion_run_id
      AND run_transition.authorization_fingerprint = target_authorization_fingerprint
      AND run_transition.commit_outcome IN ('committed', 'rolled_back')
    ORDER BY run_transition.transition_id DESC LIMIT 1 FOR UPDATE;
    IF NOT FOUND OR outcome_row.commit_outcome <> 'committed'
       OR outcome_row.supervisor_run_id IS DISTINCT FROM target_supervisor_run_id
       OR outcome_row.governed_package_sha256 IS DISTINCT FROM target_package_sha256 THEN
        RAISE EXCEPTION 'promotion requires matching committed governed outcome';
    END IF;
    SELECT package_transition.package_transition_id, package_transition.ingestion_run_id,
           package_transition.supervisor_run_id, package_transition.authorization_fingerprint,
           package_transition.governed_package_sha256, package_transition.reconciliation_sha256,
           package_transition.certificate_id, package_transition.certificate_sha256
    INTO existing
    FROM sec_source_package_transitions AS package_transition
    WHERE package_transition.package_id = target_package_id
      AND package_transition.event_kind = 'canary_promoted'
    LIMIT 1 FOR UPDATE;
    IF FOUND THEN
        IF (existing.ingestion_run_id, existing.supervisor_run_id, existing.authorization_fingerprint,
            existing.governed_package_sha256, existing.reconciliation_sha256,
            existing.certificate_id, existing.certificate_sha256)
           IS DISTINCT FROM
           (target_ingestion_run_id, target_supervisor_run_id, target_authorization_fingerprint,
            target_package_sha256, target_reconciliation_sha256,
            target_certificate_id, target_certificate_sha256) THEN
            RAISE EXCEPTION 'conflicting canary promotion for package %', target_package_id;
        END IF;
        RETURN QUERY SELECT existing.package_transition_id, target_package_id,
            existing.ingestion_run_id, existing.certificate_id, existing.reconciliation_sha256;
        RETURN;
    END IF;
    SELECT package_transition.package_id INTO existing
    FROM sec_source_package_transitions AS package_transition
    WHERE package_transition.certificate_id = target_certificate_id
      AND package_transition.event_kind = 'canary_promoted'
    LIMIT 1 FOR UPDATE;
    IF FOUND THEN RAISE EXCEPTION 'certificate % already promotes another package', target_certificate_id; END IF;
    INSERT INTO sec_source_package_transitions (
        package_id, from_state, to_state, retry_count, event_kind, ingestion_run_id,
        supervisor_run_id, authorization_fingerprint, governed_package_sha256,
        reconciliation_sha256, certificate_id, certificate_sha256
    ) VALUES (
        target_package_id, NULL, 'canary_promoted', package_row.retry_count, 'canary_promoted',
        target_ingestion_run_id, target_supervisor_run_id, target_authorization_fingerprint,
        target_package_sha256, target_reconciliation_sha256, target_certificate_id,
        target_certificate_sha256
    ) RETURNING sec_source_package_transitions.package_transition_id,
        sec_source_package_transitions.package_id, sec_source_package_transitions.ingestion_run_id,
        sec_source_package_transitions.certificate_id, sec_source_package_transitions.reconciliation_sha256
    INTO package_transition_id, package_id, ingestion_run_id, certificate_id, reconciliation_sha256;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION sec_query_governed_evidence(
    target_package_id uuid,
    target_ingestion_run_id uuid,
    target_authorization_fingerprint character(64)
)
RETURNS TABLE (
    package_id uuid,
    ingestion_run_id uuid,
    supervisor_run_id uuid,
    authorization_fingerprint character(64),
    package_sha256 character(64),
    commit_outcome text,
    reconciliation_sha256 character(64),
    certificate_id uuid,
    certificate_sha256 character(64),
    promoted_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT p.package_id, p.ingestion_run_id, p.supervisor_run_id,
           p.authorization_fingerprint, p.governed_package_sha256,
           o.commit_outcome, p.reconciliation_sha256, p.certificate_id,
           p.certificate_sha256, p.occurred_at
    FROM sec_source_package_transitions AS p
    LEFT JOIN LATERAL (
        SELECT r.commit_outcome
        FROM sec_run_transitions AS r
        WHERE r.run_id = p.ingestion_run_id
          AND r.authorization_fingerprint = p.authorization_fingerprint
          AND r.commit_outcome IN ('committed', 'rolled_back')
        ORDER BY r.transition_id DESC LIMIT 1
    ) AS o ON TRUE
    WHERE p.package_id = target_package_id
      AND p.ingestion_run_id = target_ingestion_run_id
      AND p.authorization_fingerprint = target_authorization_fingerprint
      AND p.event_kind = 'canary_promoted';
$$;

CREATE OR REPLACE FUNCTION sec_package_discovery_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.package_id IS DISTINCT FROM OLD.package_id
       OR NEW.source_family IS DISTINCT FROM OLD.source_family
       OR NEW.package_relative_path IS DISTINCT FROM OLD.package_relative_path THEN
        RAISE EXCEPTION 'package discovery identity is immutable';
    END IF;
    IF NEW.source_quarter IS DISTINCT FROM OLD.source_quarter
       OR (OLD.package_sha256 IS NOT NULL AND NEW.package_sha256 IS DISTINCT FROM OLD.package_sha256)
       OR (OLD.metadata_sha256 IS NOT NULL AND NEW.metadata_sha256 IS DISTINCT FROM OLD.metadata_sha256)
       OR (OLD.readme_sha256 IS NOT NULL AND NEW.readme_sha256 IS DISTINCT FROM OLD.readme_sha256)
       OR (OLD.run_id IS NOT NULL AND NEW.run_id IS DISTINCT FROM OLD.run_id
           AND NOT (
               OLD.package_state IN ('failed', 'quarantined', 'unsupported')
               AND NEW.package_state = 'discovered'
               AND NEW.retry_count = OLD.retry_count + 1
               AND NEW.run_id IS NULL
           ))
       OR (OLD.duplicate_of_package_id IS NOT NULL AND NEW.duplicate_of_package_id IS DISTINCT FROM OLD.duplicate_of_package_id)
       OR (OLD.reason IS NOT NULL AND NEW.reason IS DISTINCT FROM OLD.reason
           AND NOT (
               OLD.package_state IN ('failed', 'quarantined', 'unsupported')
               AND NEW.package_state = 'discovered'
               AND NEW.retry_count = OLD.retry_count + 1
               AND NEW.reason IS NULL
           )) THEN
        RAISE EXCEPTION 'package discovery immutable metadata conflict';
    END IF;
    IF NEW.package_state = OLD.package_state THEN
        IF NEW.retry_count <> OLD.retry_count THEN
            RAISE EXCEPTION 'package retry count can change only through an explicit retry';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.package_state = 'discovered'
       AND NEW.package_state IN ('loaded', 'duplicate', 'unsupported', 'quarantined', 'failed')
       AND NEW.retry_count = OLD.retry_count THEN
        RETURN NEW;
    END IF;
    IF OLD.package_state IN ('failed', 'quarantined', 'unsupported')
       AND NEW.package_state = 'discovered'
       AND NEW.retry_count = OLD.retry_count + 1
       AND NEW.reason IS NULL THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid package discovery transition % -> %', OLD.package_state, NEW.package_state;
END;
$$;

CREATE OR REPLACE FUNCTION sec_audit_package_discovery()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO sec_source_package_transitions
            (package_id, from_state, to_state, retry_count, terminal_reason)
        VALUES (NEW.package_id, NULL, NEW.package_state, NEW.retry_count, NEW.reason);
    ELSIF NEW.package_state IS DISTINCT FROM OLD.package_state THEN
        INSERT INTO sec_source_package_transitions
            (package_id, from_state, to_state, retry_count, terminal_reason)
        VALUES (NEW.package_id, OLD.package_state, NEW.package_state, NEW.retry_count,
                CASE WHEN NEW.package_state = 'discovered' THEN OLD.reason ELSE NEW.reason END);
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION sec_run_insert_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.current_state <> 'discovered' OR NEW.raw_validated_at IS NOT NULL
       OR NEW.published_at IS NOT NULL OR NEW.failure_code IS NOT NULL
       OR NEW.failure_detail IS NOT NULL OR NEW.retry_state IS NOT NULL
       OR NEW.retry_count <> 0 THEN
        RAISE EXCEPTION 'runs must be created in the discovered lifecycle state';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION sec_audit_run_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO sec_run_transitions (run_id, from_state, to_state, event_type, detail)
        VALUES (NEW.run_id, NULL, 'discovered', 'created', NULLIF(current_setting('sec.lifecycle_detail', true), ''));
    ELSE
        INSERT INTO sec_run_transitions (run_id, from_state, to_state, event_type, detail)
        VALUES (NEW.run_id, OLD.current_state, NEW.current_state,
            CASE WHEN NEW.current_state = 'raw_validated' THEN 'raw_validated'
                 WHEN NEW.current_state = 'failed' THEN 'failed'
                 WHEN OLD.current_state = 'failed' THEN 'retry'
                 ELSE 'transition' END,
            NULLIF(current_setting('sec.lifecycle_detail', true), ''));
        IF NEW.current_state = 'raw_validated' THEN
            INSERT INTO sec_validated_raw_visibility (run_id, raw_validated_at)
            VALUES (NEW.run_id, NEW.raw_validated_at);
        END IF;
    END IF;
    PERFORM set_config('sec.lifecycle_detail', '', true);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION sec_reject_append_only_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

CREATE OR REPLACE FUNCTION sec_validate_raw_visibility_marker()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE validated_at timestamptz;
BEGIN
    SELECT raw_validated_at INTO validated_at FROM sec_ingestion_runs WHERE run_id = NEW.run_id;
    IF validated_at IS NULL OR NEW.raw_validated_at IS DISTINCT FROM validated_at THEN
        RAISE EXCEPTION 'raw visibility requires a completed validation for run %', NEW.run_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS sec_source_files_raw_immutable ON sec_source_files;
CREATE TRIGGER sec_source_files_raw_immutable BEFORE INSERT OR UPDATE OR DELETE ON sec_source_files
FOR EACH ROW EXECUTE FUNCTION sec_lock_manifest_run_lineage();
DROP TRIGGER IF EXISTS sec_source_packages_discovery_guard ON sec_source_packages;
CREATE TRIGGER sec_source_packages_discovery_guard BEFORE UPDATE ON sec_source_packages
FOR EACH ROW EXECUTE FUNCTION sec_package_discovery_guard();
DROP TRIGGER IF EXISTS sec_source_packages_discovery_audit ON sec_source_packages;
CREATE TRIGGER sec_source_packages_discovery_audit AFTER INSERT OR UPDATE ON sec_source_packages
FOR EACH ROW EXECUTE FUNCTION sec_audit_package_discovery();
DROP TRIGGER IF EXISTS sec_table_reconciliations_raw_immutable ON sec_table_reconciliations;
CREATE TRIGGER sec_table_reconciliations_raw_immutable BEFORE INSERT OR UPDATE OR DELETE ON sec_table_reconciliations
FOR EACH ROW EXECUTE FUNCTION sec_lock_manifest_run_lineage();
DROP TRIGGER IF EXISTS sec_row_issues_raw_immutable ON sec_row_issues;
CREATE TRIGGER sec_row_issues_raw_immutable BEFORE INSERT OR UPDATE OR DELETE ON sec_row_issues
FOR EACH ROW EXECUTE FUNCTION sec_lock_issue_run_lineage();
DROP TRIGGER IF EXISTS sec_row_issues_active_disposition ON sec_row_issues;
CREATE TRIGGER sec_row_issues_active_disposition BEFORE INSERT ON sec_row_issues
FOR EACH ROW EXECUTE FUNCTION sec_check_issue_active_disposition();
DROP TRIGGER IF EXISTS sec_row_issues_append_only ON sec_row_issues;
CREATE TRIGGER sec_row_issues_append_only BEFORE UPDATE OR DELETE ON sec_row_issues
FOR EACH ROW EXECUTE FUNCTION sec_reject_append_only_mutation();
DROP TRIGGER IF EXISTS sec_run_transitions_append_only ON sec_run_transitions;
CREATE TRIGGER sec_run_transitions_append_only BEFORE UPDATE OR DELETE ON sec_run_transitions
FOR EACH ROW EXECUTE FUNCTION sec_reject_append_only_mutation();
DROP TRIGGER IF EXISTS sec_source_package_transitions_append_only ON sec_source_package_transitions;
CREATE TRIGGER sec_source_package_transitions_append_only BEFORE UPDATE OR DELETE ON sec_source_package_transitions
FOR EACH ROW EXECUTE FUNCTION sec_reject_append_only_mutation();
DROP TRIGGER IF EXISTS sec_validated_raw_visibility_eligible ON sec_validated_raw_visibility;
CREATE TRIGGER sec_validated_raw_visibility_eligible BEFORE INSERT OR UPDATE ON sec_validated_raw_visibility
FOR EACH ROW EXECUTE FUNCTION sec_validate_raw_visibility_marker();
DROP TRIGGER IF EXISTS sec_validated_raw_visibility_append_only ON sec_validated_raw_visibility;
CREATE TRIGGER sec_validated_raw_visibility_append_only BEFORE UPDATE OR DELETE ON sec_validated_raw_visibility
FOR EACH ROW EXECUTE FUNCTION sec_reject_append_only_mutation();
DROP TRIGGER IF EXISTS sec_ingestion_runs_published_immutable ON sec_ingestion_runs;
DROP TRIGGER IF EXISTS sec_ingestion_runs_raw_validation_irreversible ON sec_ingestion_runs;
DROP TRIGGER IF EXISTS sec_ingestion_runs_lifecycle_guard ON sec_ingestion_runs;
CREATE TRIGGER sec_ingestion_runs_lifecycle_guard BEFORE UPDATE ON sec_ingestion_runs
FOR EACH ROW EXECUTE FUNCTION sec_run_lifecycle_guard();
DROP TRIGGER IF EXISTS sec_ingestion_runs_insert_guard ON sec_ingestion_runs;
CREATE TRIGGER sec_ingestion_runs_insert_guard BEFORE INSERT ON sec_ingestion_runs
FOR EACH ROW EXECUTE FUNCTION sec_run_insert_guard();
DROP TRIGGER IF EXISTS sec_ingestion_runs_lifecycle_audit ON sec_ingestion_runs;
CREATE TRIGGER sec_ingestion_runs_lifecycle_audit AFTER INSERT OR UPDATE ON sec_ingestion_runs
FOR EACH ROW EXECUTE FUNCTION sec_audit_run_lifecycle();

-- The installer only defines the routines.  Task 4 grants the dedicated
-- runtime login explicitly after ownership and ACL attestation.
REVOKE ALL ON FUNCTION sec_raw_validation_token_present(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_run_lifecycle_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_validate_raw_run(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_audit_package_discovery() FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_audit_run_lifecycle() FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_record_commit_outcome(uuid,uuid,character,character,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_promote_certified_canary_package(uuid,uuid,uuid,character,character,character,uuid,character) FROM PUBLIC;
REVOKE ALL ON FUNCTION sec_query_governed_evidence(uuid,uuid,character) FROM PUBLIC;
