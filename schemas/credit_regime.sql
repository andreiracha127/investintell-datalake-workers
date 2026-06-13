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
    state         text           NOT NULL,           -- 'risk_on' | 'risk_off' (BINÁRIO, histerese)
    hyg_close     numeric(14,6),                     -- adjClose Tiingo (proveniência)
    ief_close     numeric(14,6),
    ratio         numeric(14,8)  NOT NULL,           -- hyg/ief
    p20_5y        numeric(14,8),                     -- banda de ENTRADA (p20 default); NULL warmup
    p_exit_5y     numeric(14,8),                     -- banda de SAÍDA (histerese); == p20_5y se exit==entry
    stress_score  numeric(6,3),                      -- 0–100 graduado (modo low-drawdown); NULL warmup
    n_window      integer        NOT NULL,           -- obs na janela móvel (máx 1260)
    flip          boolean        NOT NULL DEFAULT false,
    computed_at   timestamptz    NOT NULL DEFAULT now(),

    CONSTRAINT credit_regime_daily_pkey PRIMARY KEY (regime_date),
    CONSTRAINT ck_credit_regime_state CHECK (state IN ('risk_on', 'risk_off'))
);

-- Migração idempotente para tabelas já materializadas (o worker grava estas
-- colunas a cada run; o detector binário/histerese e o score graduado são
-- aditivos — NULL durante o warmup). Veredito: histerese assimétrica é o único
-- componente do legado com valor comprovado; stress_score alimenta o modo
-- low-drawdown consumido pelo Light (GET /macro/regime?low_drawdown_mode=true).
ALTER TABLE credit_regime_daily ADD COLUMN IF NOT EXISTS p_exit_5y    numeric(14,8);
ALTER TABLE credit_regime_daily ADD COLUMN IF NOT EXISTS stress_score numeric(6,3);

CREATE INDEX IF NOT EXISTS credit_regime_daily_flip_idx
    ON credit_regime_daily (regime_date DESC) WHERE flip;
