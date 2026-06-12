-- credit_regime worker — destination table (idempotent DDL)
-- Detector binário de stress de crédito (Frente B re-escopada, ADENDO §6 +
-- veredito do backtest 2026-06-11-macro-regime-backtest.md): proxy de preço
-- HYG/IEF (closes ajustados Tiingo) contra o p20 móvel de 5 anos. O composite
-- legado (macro_regime_snapshot) foi REFUTADO como gatilho — o Light consome
-- ESTA tabela via GET /macro/regime.
--
-- A série inteira é recomputada a cada run (closes ajustados mudam
-- retroativamente quando HYG distribui dividendos), por isso o upsert é
-- DO UPDATE em todas as colunas derivadas.
--
-- Apply against the cloud:  psql "$DATABASE_URL" -f schemas/credit_regime.sql

CREATE TABLE IF NOT EXISTS credit_regime_daily (
    regime_date   date           NOT NULL,
    state         text           NOT NULL,           -- 'risk_on' | 'risk_off'
    hyg_close     numeric(14,6),                     -- adjClose Tiingo (proveniência)
    ief_close     numeric(14,6),
    ratio         numeric(14,8)  NOT NULL,           -- hyg/ief
    p20_5y        numeric(14,8),                     -- NULL durante warmup (<252 obs)
    n_window      integer        NOT NULL,           -- obs na janela móvel (máx 1260)
    flip          boolean        NOT NULL DEFAULT false,
    computed_at   timestamptz    NOT NULL DEFAULT now(),

    CONSTRAINT credit_regime_daily_pkey PRIMARY KEY (regime_date),
    CONSTRAINT ck_credit_regime_state CHECK (state IN ('risk_on', 'risk_off'))
);

CREATE INDEX IF NOT EXISTS credit_regime_daily_flip_idx
    ON credit_regime_daily (regime_date DESC) WHERE flip;
