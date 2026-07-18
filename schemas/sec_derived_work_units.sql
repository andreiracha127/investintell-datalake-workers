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
    UNIQUE (run_id, product, publication_version, unit_key, input_fingerprint),
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

CREATE INDEX IF NOT EXISTS sec_derived_work_units_resume_idx
    ON sec_derived_work_units (run_id, state, product, publication_version, unit_key);
