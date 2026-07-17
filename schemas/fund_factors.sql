-- schemas/fund_factors.sql
-- A1 — exposições de fatores por fundo (OLS de retornos mensais do NAV vs
-- factor_model_fits.factor_returns). GLOBAL (organization_id NULL). Upsert por
-- (instrument_id, factor, as_of). Aplicar schemas/factor_model.sql primeiro,
-- pois fit_id referencia factor_model_fits.
CREATE TABLE IF NOT EXISTS fund_factor_exposures (
    instrument_id    uuid    NOT NULL,
    factor           text    NOT NULL,
    factor_index     integer,
    as_of            date    NOT NULL,
    fit_id            uuid REFERENCES factor_model_fits(fit_id) ON DELETE CASCADE,
    beta             numeric(14, 8),
    t_stat           numeric(14, 8),
    significance     text,
    n_observations   integer,
    r_squared        numeric,
    organization_id  uuid,
    computed_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ux_fund_factor_exposures_pk
        UNIQUE NULLS NOT DISTINCT (instrument_id, factor, as_of, organization_id)
);

CREATE INDEX IF NOT EXISTS fund_factor_exposures_iid_idx
    ON fund_factor_exposures (instrument_id, as_of DESC);

ALTER TABLE fund_factor_exposures
    ADD COLUMN IF NOT EXISTS factor_index INTEGER,
    ADD COLUMN IF NOT EXISTS fit_id UUID REFERENCES factor_model_fits(fit_id)
        ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS n_observations INTEGER,
    ADD COLUMN IF NOT EXISTS r_squared NUMERIC;

CREATE INDEX IF NOT EXISTS fund_factor_exposures_fit_idx
    ON fund_factor_exposures (fit_id, instrument_id, factor_index);
