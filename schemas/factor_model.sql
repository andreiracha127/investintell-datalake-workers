-- factor_model.sql — DDL idempotente para factor_model_fits (worker IPCA).
--
-- Extraído do DB-mãe (localhost:5434, investintell_alloc) via
-- information_schema; reproduzido aqui de forma idempotente e aplicado no
-- TimescaleDB Cloud (Investintell-Prod). Tabela GLOBAL (sem RLS) — um fit
-- IPCA descreve o universo inteiro, não um fundo.
--
-- Formato dos campos JSONB (compatível com o legado, ver app/jobs/workers/
-- ipca_estimation.py):
--   gamma_loadings : array 2D  L x K  (L = nº de características, K = nº de fatores)
--                    linha i = loading da característica i nos K fatores latentes.
--   factor_returns : objeto {"dates": [ISO...], "values": [[...K linhas x T cols...]]}
--                    values[k] = série temporal (T) do retorno do fator latente k.
--
-- Chave idempotente: (engine, asset_class, universe_hash, fit_date,
-- content_hash). Rerun idêntico reutiliza o fit_id; conteúdo diferente cria
-- uma nova versão imutável e preserva a linhagem das exposições dependentes.

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

CREATE TABLE IF NOT EXISTS factor_model_fits (
    fit_id          uuid        NOT NULL DEFAULT gen_random_uuid(),
    engine          varchar     NOT NULL,
    fit_date        date        NOT NULL,
    universe_hash   varchar     NOT NULL,
    k_factors       integer     NOT NULL,
    gamma_loadings  jsonb       NOT NULL,
    factor_returns  jsonb       NOT NULL,
    in_sample_r_squared numeric,
    oos_r_squared   numeric,
    converged       boolean     NOT NULL,
    n_iterations    integer     NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    asset_class     varchar     NOT NULL DEFAULT 'Equity',
    sample_start    date,
    sample_end      date,
    n_observations  integer,
    n_instruments   integer,
    feature_names   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    selection_metadata jsonb    NOT NULL DEFAULT '{}'::jsonb,
    degraded        boolean     NOT NULL DEFAULT false,
    degraded_reason text,
    content_hash     text        NOT NULL,
    production_fit  boolean     NOT NULL DEFAULT false,
    CONSTRAINT factor_model_fits_pkey PRIMARY KEY (fit_id)
);

-- Lookup index do legado (engine, asset_class, fit_date).
CREATE INDEX IF NOT EXISTS ix_factor_model_fits_lookup
    ON factor_model_fits (engine, asset_class, fit_date);

-- ---------------------------------------------------------------------------
-- T3B-3: Gamma drift columns. Procrustes-aligned relative Frobenius drift of
-- this fit's Gamma vs. the previous fit for the same (engine, asset_class,
-- universe_hash). NULL until the gamma_drift monitor runs (>= 2 fits needed).
-- Idempotent ADD COLUMN IF NOT EXISTS — safe to re-run.
-- ---------------------------------------------------------------------------
ALTER TABLE factor_model_fits
    ADD COLUMN IF NOT EXISTS gamma_drift_vs_prior NUMERIC,
    ADD COLUMN IF NOT EXISTS drift_alert          BOOLEAN,
    ADD COLUMN IF NOT EXISTS in_sample_r_squared  NUMERIC,
    ADD COLUMN IF NOT EXISTS sample_start          DATE,
    ADD COLUMN IF NOT EXISTS sample_end            DATE,
    ADD COLUMN IF NOT EXISTS n_observations         INTEGER,
    ADD COLUMN IF NOT EXISTS n_instruments          INTEGER,
    ADD COLUMN IF NOT EXISTS feature_names          JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS selection_metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS degraded               BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS degraded_reason        TEXT,
    ADD COLUMN IF NOT EXISTS content_hash           TEXT,
    ADD COLUMN IF NOT EXISTS production_fit         BOOLEAN NOT NULL DEFAULT false;

UPDATE factor_model_fits
SET content_hash = md5(
    gamma_loadings::text || factor_returns::text || fit_id::text
)
WHERE content_hash IS NULL;

ALTER TABLE factor_model_fits
    ALTER COLUMN content_hash SET NOT NULL;

DROP INDEX IF EXISTS uq_factor_model_fits_natural;

CREATE UNIQUE INDEX IF NOT EXISTS uq_factor_model_fits_content
    ON factor_model_fits (
        engine, asset_class, universe_hash, fit_date, content_hash
    );

-- D_i mensal para futura montagem Sigma = B F B' + D em solvers/otimizadores.
CREATE TABLE IF NOT EXISTS factor_model_specific_variances (
    fit_id             uuid        NOT NULL REFERENCES factor_model_fits(fit_id)
                                  ON DELETE CASCADE,
    instrument_id      uuid        NOT NULL,
    variance_monthly   numeric     NOT NULL CHECK (variance_monthly >= 0),
    n_observations     integer     NOT NULL CHECK (n_observations >= 2),
    computed_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT factor_model_specific_variances_pkey
        PRIMARY KEY (fit_id, instrument_id)
);

CREATE INDEX IF NOT EXISTS ix_factor_model_specific_variances_instrument
    ON factor_model_specific_variances (instrument_id, fit_id);

-- B_i = z_i Gamma no último corte do fit. Evita reconstruir ranks com um
-- universo diferente e fornece diretamente os loadings para futuros solvers.
CREATE TABLE IF NOT EXISTS factor_model_instrument_exposures (
    fit_id                   uuid        NOT NULL REFERENCES factor_model_fits(fit_id)
                                        ON DELETE CASCADE,
    instrument_id            uuid        NOT NULL,
    as_of                    date        NOT NULL,
    ranked_characteristics   jsonb       NOT NULL,
    factor_exposures         jsonb       NOT NULL,
    computed_at              timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT factor_model_instrument_exposures_pkey
        PRIMARY KEY (fit_id, instrument_id)
);

CREATE INDEX IF NOT EXISTS ix_factor_model_instrument_exposures_instrument
    ON factor_model_instrument_exposures (instrument_id, fit_id);
