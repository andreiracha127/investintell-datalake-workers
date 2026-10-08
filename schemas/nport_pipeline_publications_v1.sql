-- Worker-owned receipts, atomically committed with each derived publication.
-- Apply as worker_writer; also installed by the idempotent worker bootstrap.
CREATE TABLE IF NOT EXISTS public.nport_pipeline_publications (
    stage text PRIMARY KEY CHECK (stage IN ('characteristics', 'lookthrough')),
    input_signature text NOT NULL,
    published_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
