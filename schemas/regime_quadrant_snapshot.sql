-- regime_quadrant_snapshot — versioned QuadrantSnapshot (freeze v1 §3/§7/§10).
--
-- The STRATEGIC quadrant (macro, point-in-time) and the MARKET-implied challenger
-- both write here, distinguished by model_version. This is a DIFFERENT dimension
-- from regime_gate_daily (the risk gate) — they have independent SLA/staleness
-- (§1.1). PK is snapshot_id, a deterministic uuid5 over
-- (model_version | as_of | source_vintage_hash | previous_snapshot_id) — owner
-- decision: previous_snapshot_id is part of the identity because the latched
-- hysteresis result depends on the predecessor. The table is IMMUTABLE: re-running
-- the same model with the same inputs AND predecessor reproduces the same id, so the
-- daily recompute upserts in place instead of exploding rows.
--
-- status_at_compute is PERSISTED and IMMUTABLE; effective_status (stale) is
-- derived AT READ. quadrant (consumable) is non-NULL ONLY when status='valid';
-- candidate_quadrant (audit/UI) is the instantaneous classification.
-- growth_internal_sign/inflation_internal_sign persist the LATCHED memory so the
-- next run can resume the hysteresis chain (the worker reads the last row by
-- as_of/available_at). regime_quadrant_current_v exposes ONLY the consumable,
-- unexpired snapshot; the full history stays in regime_quadrant_snapshot.
--
-- Apply against the cloud: psql "$DATABASE_URL" -f schemas/regime_quadrant_snapshot.sql

CREATE TABLE IF NOT EXISTS regime_quadrant_snapshot (
    snapshot_id                     uuid          NOT NULL,        -- uuid5(namespace, model|as_of|vintage|prev)
    previous_snapshot_id            uuid,                          -- NULL at genesis; closes the latched chain
    -- consumable + candidate classification
    quadrant                        text,                          -- recovery|expansion|slowdown|contraction; NULL unless valid
    candidate_quadrant              text,                          -- instantaneous (audit/UI)
    candidate_confidence            numeric(6,4),                  -- min over axes; NULL if unavailable/invalid
    -- growth axis diagnostics (§3 AxisDiagnostics)
    growth_score                    numeric(18,8),
    growth_sign                     smallint,                      -- -1|1|NULL (post-hysteresis EFFECTIVE/consumable sign)
    growth_internal_sign            smallint,                      -- -1|1|NULL (LATCHED memory carried to next run)
    growth_candidate_confidence     numeric(6,4),
    growth_margin                   numeric(18,8),
    growth_uncertainty_raw          numeric(18,8),
    growth_uncertainty_adjusted     numeric(18,8),
    -- inflation axis diagnostics
    inflation_score                 numeric(18,8),
    inflation_sign                  smallint,
    inflation_internal_sign         smallint,
    inflation_candidate_confidence  numeric(6,4),
    inflation_margin                numeric(18,8),
    inflation_uncertainty_raw       numeric(18,8),
    inflation_uncertainty_adjusted  numeric(18,8),
    -- aggregate quality (§4) and transition
    coverage_quality                numeric(6,4)  NOT NULL,
    freshness_quality               numeric(6,4)  NOT NULL,
    source_health_quality           numeric(6,4)  NOT NULL,
    transition_pending              boolean       NOT NULL DEFAULT false,
    transition_reason               text,
    -- point-in-time + staleness (§8/§9)
    as_of                           date          NOT NULL,
    available_at                    timestamptz   NOT NULL,
    computed_at                     timestamptz   NOT NULL DEFAULT now(),
    data_stale_after                timestamptz   NOT NULL,
    pipeline_stale_after            timestamptz   NOT NULL,
    stale_after                     timestamptz   NOT NULL,
    -- status + provenance (§3/§36)
    status_at_compute               text          NOT NULL,
    model_version                   text          NOT NULL,
    confidence_model_version        text          NOT NULL,
    confidence_method               text          NOT NULL,
    source_vintage_hash             text          NOT NULL,

    CONSTRAINT regime_quadrant_snapshot_pkey PRIMARY KEY (snapshot_id),
    -- owner decision: identity includes previous_snapshot_id (the hysteresis result
    -- depends on the predecessor); re-running the same model with the same inputs AND
    -- predecessor yields the same uuid -> the daily upsert stays idempotent.
    -- NULLS NOT DISTINCT so the genesis row (previous=NULL) is also de-duplicated.
    CONSTRAINT uq_regime_quadrant_version
        UNIQUE NULLS NOT DISTINCT
        (model_version, as_of, source_vintage_hash, previous_snapshot_id),
    CONSTRAINT ck_rqs_status_domain CHECK (
        status_at_compute IN ('valid', 'low_confidence', 'unavailable', 'invalid')
    ),
    CONSTRAINT ck_rqs_quadrant_domain CHECK (
        quadrant IS NULL OR quadrant IN
        ('recovery', 'expansion', 'slowdown', 'contraction')
    ),
    CONSTRAINT ck_rqs_candidate_domain CHECK (
        candidate_quadrant IS NULL OR candidate_quadrant IN
        ('recovery', 'expansion', 'slowdown', 'contraction')
    ),
    -- §7 coherence: valid <=> fully classified, confident, no pending; else quadrant NULL.
    CONSTRAINT ck_rqs_valid_coherence CHECK (
        (status_at_compute = 'valid' AND quadrant IS NOT NULL
            AND candidate_quadrant IS NOT NULL AND candidate_confidence IS NOT NULL
            AND candidate_confidence >= 0.70 AND transition_pending = FALSE)
        OR (status_at_compute <> 'valid' AND quadrant IS NULL)
    ),
    -- §7: unavailable/invalid carry NO confidence.
    CONSTRAINT ck_rqs_unavailable_no_confidence CHECK (
        status_at_compute NOT IN ('unavailable', 'invalid')
        OR candidate_confidence IS NULL
    ),
    -- quality fields in [0,1]
    CONSTRAINT ck_rqs_coverage_range CHECK (coverage_quality BETWEEN 0 AND 1),
    CONSTRAINT ck_rqs_freshness_range CHECK (freshness_quality BETWEEN 0 AND 1),
    CONSTRAINT ck_rqs_health_range CHECK (source_health_quality BETWEEN 0 AND 1),
    -- §7/§9 temporal ordering
    CONSTRAINT ck_rqs_stale_le_data CHECK (stale_after <= data_stale_after),
    CONSTRAINT ck_rqs_stale_le_pipeline CHECK (stale_after <= pipeline_stale_after),
    CONSTRAINT ck_rqs_computed_ge_available CHECK (computed_at >= available_at),
    CONSTRAINT ck_rqs_asof_le_available CHECK (as_of <= available_at),
    -- §7 provenance non-empty
    CONSTRAINT ck_rqs_version_nonempty CHECK (
        length(model_version) > 0 AND length(source_vintage_hash) > 0
    )
);

-- §6 consumable read: filter valid + fresh + confident, newest available_at first.
CREATE INDEX IF NOT EXISTS regime_quadrant_snapshot_consume_idx
    ON regime_quadrant_snapshot (model_version, available_at DESC)
    WHERE status_at_compute = 'valid' AND quadrant IS NOT NULL;

-- Latched-chain read: newest snapshot per model_version (worker resumes hysteresis).
CREATE INDEX IF NOT EXISTS regime_quadrant_snapshot_latest_idx
    ON regime_quadrant_snapshot (model_version, as_of DESC, available_at DESC);

-- Operational view: ONLY the consumable, unexpired snapshot per model_version.
-- The backend reader selects from this view; the full audit trail lives in the base
-- table. now() at query time excludes snapshots already past stale_after.
CREATE OR REPLACE VIEW regime_quadrant_current_v AS
SELECT DISTINCT ON (model_version) *
FROM regime_quadrant_snapshot
WHERE status_at_compute = 'valid'
  AND quadrant IS NOT NULL
  AND candidate_confidence >= 0.70
  AND stale_after > now()
ORDER BY model_version, available_at DESC;

-- §10 per-indicator audit + per-observation lineage: one row per (snapshot, axis,
-- indicator). Carries the individual observation period / vintage so the audit can
-- distinguish ambiguous-macro / missing-coverage / late-source / ingestion-fault /
-- anomalous-revision / high-statistical-uncertainty (freeze §10). The snapshot row
-- itself stores only aggregates; individual observation dates live HERE, not in as_of.
CREATE TABLE IF NOT EXISTS regime_quadrant_indicator_audit (
    snapshot_id        uuid          NOT NULL,
    axis               text          NOT NULL,   -- 'growth' | 'inflation'
    series_id          text          NOT NULL,
    z_score            numeric(18,8),            -- standardized contribution z_k
    weight             numeric(18,8),            -- renormalized w_k
    coverage           numeric(6,4),
    freshness          numeric(6,4),
    source_health      numeric(6,4),
    anomaly            text,                      -- NULL = none; else a short tag
    observation_period date,                      -- the obs date that fed z_k
    vintage_id         text,                      -- vintage identity (lineage)
    revision_number    integer,                   -- revision lineage
    PRIMARY KEY (snapshot_id, axis, series_id),
    CONSTRAINT ck_rqia_axis CHECK (axis IN ('growth', 'inflation'))
);
