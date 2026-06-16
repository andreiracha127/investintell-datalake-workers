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
-- Chave natural para upsert idempotente: (engine, asset_class, universe_hash,
-- fit_date). Reexecutar o worker para a mesma data/universo substitui o fit
-- em vez de duplicar (o legado fazia INSERT puro e acumulava duplicatas — ver
-- as 2 linhas idênticas no DB-mãe; aqui corrigimos para upsert).

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

CREATE TABLE IF NOT EXISTS factor_model_fits (
    fit_id          uuid        NOT NULL DEFAULT gen_random_uuid(),
    engine          varchar     NOT NULL,
    fit_date        date        NOT NULL,
    universe_hash   varchar     NOT NULL,
    k_factors       integer     NOT NULL,
    gamma_loadings  jsonb       NOT NULL,
    factor_returns  jsonb       NOT NULL,
    oos_r_squared   numeric,
    converged       boolean     NOT NULL,
    n_iterations    integer     NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    asset_class     varchar     NOT NULL DEFAULT 'Equity',
    CONSTRAINT factor_model_fits_pkey PRIMARY KEY (fit_id)
);

-- Lookup index do legado (engine, asset_class, fit_date).
CREATE INDEX IF NOT EXISTS ix_factor_model_fits_lookup
    ON factor_model_fits (engine, asset_class, fit_date);

-- Chave natural única para o upsert ON CONFLICT do worker reconstruído.
-- (Não existia no legado, por isso ele acumulava duplicatas.)
CREATE UNIQUE INDEX IF NOT EXISTS uq_factor_model_fits_natural
    ON factor_model_fits (engine, asset_class, universe_hash, fit_date);

-- ---------------------------------------------------------------------------
-- T3B-3: Gamma drift columns. Procrustes-aligned relative Frobenius drift of
-- this fit's Gamma vs. the previous fit for the same (engine, asset_class,
-- universe_hash). NULL until the gamma_drift monitor runs (>= 2 fits needed).
-- Idempotent ADD COLUMN IF NOT EXISTS — safe to re-run.
-- ---------------------------------------------------------------------------
ALTER TABLE factor_model_fits
    ADD COLUMN IF NOT EXISTS gamma_drift_vs_prior NUMERIC,
    ADD COLUMN IF NOT EXISTS drift_alert          BOOLEAN;
