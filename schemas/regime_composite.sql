-- regime_composite worker — destination table (idempotent DDL)
-- Detector vote2of3 (Frente B, evolução do detector): ensemble por VOTOS validado
-- no backtest (2026-06-12-regime-detector-alternatives-backtest.md, RegimeAltVote:
-- Sharpe 0,549 / DD 25,3% / CAGR 12,30% / ~16 flips). risk_off ⇔ ≥2 votos entre
-- credit (HYG/IEF < p20 5y, lido de credit_regime_daily), trend (SPY mensal < SMA10m)
-- e nfci (Chicago Fed > 0 entra / < −0,05 sai). Estados BINÁRIOS — sem caution.
--
-- credit_regime_daily é mantido INTACTO (é 1 dos votos). A série inteira é
-- recomputada a cada run (closes ajustados mudam retroativamente), por isso o
-- upsert é DO UPDATE em todas as colunas derivadas.
--
-- Apply against the cloud:  psql "$DATABASE_URL" -f schemas/regime_composite.sql

CREATE TABLE IF NOT EXISTS regime_composite_daily (
    regime_date   date           NOT NULL,
    state         text           NOT NULL,           -- 'risk_on' | 'risk_off'
    credit_vote   boolean        NOT NULL,           -- HYG/IEF < p20 5y
    trend_vote    boolean        NOT NULL,           -- SPY mensal < SMA10m
    nfci_vote     boolean        NOT NULL,           -- NFCI > 0 (histerese)
    vote_count    smallint       NOT NULL,           -- 0..3
    ratio         numeric(14,8),                     -- proveniência do voto de crédito
    p20_5y        numeric(14,8),
    nfci          numeric(10,4),                     -- valor NFCI (forward-filled)
    flip          boolean        NOT NULL DEFAULT false,
    computed_at   timestamptz    NOT NULL DEFAULT now(),

    CONSTRAINT regime_composite_daily_pkey PRIMARY KEY (regime_date),
    CONSTRAINT ck_regime_composite_state CHECK (state IN ('risk_on', 'risk_off')),
    CONSTRAINT ck_regime_composite_votes CHECK (vote_count BETWEEN 0 AND 3)
);

CREATE INDEX IF NOT EXISTS regime_composite_daily_flip_idx
    ON regime_composite_daily (regime_date DESC) WHERE flip;
