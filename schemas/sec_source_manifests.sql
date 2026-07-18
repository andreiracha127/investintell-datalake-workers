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
BEGIN
    IF NEW.status NOT IN ('quarantined', 'rejected') THEN
        RETURN NEW;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.source_file_id::text || ':' || NEW.source_row_number::text, 0));
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
DECLARE validated_at timestamptz;
BEGIN
    PERFORM 1 FROM sec_ingestion_runs WHERE run_id = target_run_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown SEC run %', target_run_id; END IF;
    IF (SELECT current_state FROM sec_ingestion_runs WHERE run_id = target_run_id) <> 'loading' THEN
        RAISE EXCEPTION 'raw validation requires loading state';
    END IF;
    -- VOLATILE + parent row lock intentionally obtains current child visibility
    -- after any writer that held the same lineage lock has committed.
    IF NOT sec_raw_run_reconciles(target_run_id) THEN
        RAISE EXCEPTION 'raw reconciliation failed for run %', target_run_id;
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
