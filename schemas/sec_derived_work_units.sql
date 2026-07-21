-- Restartable control-plane checkpoints for derived SEC work.
-- A work unit records progress only; it never publishes an analytical product.

CREATE TABLE IF NOT EXISTS sec_derived_work_units (
    work_unit_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    product text NOT NULL CHECK (product <> ''),
    publication_version integer NOT NULL CHECK (publication_version > 0),
    unit_key text NOT NULL CHECK (unit_key <> ''),
    input_fingerprint char(64) NOT NULL CHECK (input_fingerprint ~ '^[0-9a-f]{64}$'),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'running', 'completed', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_token uuid,
    started_at timestamptz,
    heartbeat_at timestamptz,
    completed_at timestamptz,
    failure_code text,
    failure_detail text,
    output_fingerprint char(64) CHECK (output_fingerprint IS NULL OR output_fingerprint ~ '^[0-9a-f]{64}$'),
    evidence jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        state <> 'pending' OR (
            lease_token IS NULL AND started_at IS NULL AND heartbeat_at IS NULL
            AND completed_at IS NULL AND failure_code IS NULL AND failure_detail IS NULL
            AND output_fingerprint IS NULL AND evidence IS NULL
        )
    ),
    CHECK (
        state <> 'running' OR (
            lease_token IS NOT NULL AND started_at IS NOT NULL AND heartbeat_at IS NOT NULL
            AND completed_at IS NULL AND failure_code IS NULL AND failure_detail IS NULL
            AND output_fingerprint IS NULL AND evidence IS NULL
        )
    ),
    CHECK (
        state <> 'completed' OR (
            lease_token IS NULL AND started_at IS NULL AND heartbeat_at IS NULL
            AND completed_at IS NOT NULL AND failure_code IS NULL AND failure_detail IS NULL
            AND output_fingerprint IS NOT NULL AND evidence IS NOT NULL
        )
    ),
    CHECK (
        state <> 'failed' OR (
            lease_token IS NULL AND started_at IS NULL AND heartbeat_at IS NULL
            AND completed_at IS NULL AND NULLIF(failure_code, '') IS NOT NULL
            AND output_fingerprint IS NULL AND evidence IS NULL
        )
    )
);

-- Replace the Phase 4 draft identity, which incorrectly treated changed input
-- as a second unit instead of a conflict for the same business unit.
DO $$
DECLARE old_constraint text;
BEGIN
    FOR old_constraint IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'sec_derived_work_units'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) LIKE '%input_fingerprint%'
    LOOP
        EXECUTE format('ALTER TABLE sec_derived_work_units DROP CONSTRAINT %I', old_constraint);
    END LOOP;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS sec_derived_work_units_business_identity_uidx
    ON sec_derived_work_units (run_id, product, publication_version, unit_key);

CREATE INDEX IF NOT EXISTS sec_derived_work_units_resume_idx
    ON sec_derived_work_units (run_id, state, product, publication_version, unit_key);

CREATE OR REPLACE FUNCTION sec_derived_work_unit_lifecycle_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'derived work unit lifecycle is immutable: delete is forbidden';
    END IF;

    IF ROW(NEW.work_unit_id, NEW.run_id, NEW.product, NEW.publication_version,
           NEW.unit_key, NEW.input_fingerprint, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.work_unit_id, OLD.run_id, OLD.product, OLD.publication_version,
           OLD.unit_key, OLD.input_fingerprint, OLD.created_at) THEN
        RAISE EXCEPTION 'derived work unit lifecycle is immutable: identity changed';
    END IF;

    IF OLD.state IN ('pending', 'failed') AND NEW.state = 'running'
       AND NEW.attempt_count = OLD.attempt_count + 1
       AND NEW.updated_at >= OLD.updated_at THEN
        RETURN NEW;
    END IF;

    IF OLD.state = 'running' AND NEW.state = 'running'
       AND NEW.attempt_count = OLD.attempt_count
       AND NEW.lease_token = OLD.lease_token
       AND NEW.started_at = OLD.started_at
       AND NEW.heartbeat_at >= OLD.heartbeat_at
       AND NEW.completed_at IS NOT DISTINCT FROM OLD.completed_at
       AND NEW.failure_code IS NOT DISTINCT FROM OLD.failure_code
       AND NEW.failure_detail IS NOT DISTINCT FROM OLD.failure_detail
       AND NEW.output_fingerprint IS NOT DISTINCT FROM OLD.output_fingerprint
       AND NEW.evidence IS NOT DISTINCT FROM OLD.evidence
       AND NEW.updated_at >= OLD.updated_at THEN
        RETURN NEW;
    END IF;

    -- Explicit stale recovery rotates the lease and records a new attempt.
    IF OLD.state = 'running' AND NEW.state = 'running'
       AND NEW.attempt_count = OLD.attempt_count + 1
       AND NEW.lease_token IS DISTINCT FROM OLD.lease_token
       AND NEW.started_at >= OLD.started_at
       AND NEW.heartbeat_at >= OLD.heartbeat_at
       AND NEW.completed_at IS NOT DISTINCT FROM OLD.completed_at
       AND NEW.failure_code IS NOT DISTINCT FROM OLD.failure_code
       AND NEW.failure_detail IS NOT DISTINCT FROM OLD.failure_detail
       AND NEW.output_fingerprint IS NOT DISTINCT FROM OLD.output_fingerprint
       AND NEW.evidence IS NOT DISTINCT FROM OLD.evidence
       AND NEW.updated_at >= OLD.updated_at THEN
        RETURN NEW;
    END IF;

    IF OLD.state = 'running' AND NEW.state IN ('failed', 'completed')
       AND NEW.attempt_count = OLD.attempt_count
       AND NEW.updated_at >= OLD.updated_at THEN
        RETURN NEW;
    END IF;

    IF OLD.state = 'completed' AND NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'derived work unit lifecycle transition is forbidden: % -> %', OLD.state, NEW.state;
END;
$$;

DROP TRIGGER IF EXISTS sec_derived_work_units_lifecycle_guard ON sec_derived_work_units;
CREATE TRIGGER sec_derived_work_units_lifecycle_guard
BEFORE UPDATE OR DELETE ON sec_derived_work_units
FOR EACH ROW EXECUTE FUNCTION sec_derived_work_unit_lifecycle_guard();
