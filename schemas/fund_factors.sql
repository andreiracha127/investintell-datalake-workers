-- schemas/fund_factors.sql
-- A1 — exposições de fatores por fundo (OLS de retornos mensais do NAV vs
-- factor_model_fits.factor_returns). GLOBAL (organization_id NULL). Upsert por
-- (instrument_id, factor, as_of). Apply: psql "$DATABASE_URL" -f schemas/fund_factors.sql
CREATE TABLE IF NOT EXISTS fund_factor_exposures (
    instrument_id    uuid    NOT NULL,
    factor           text    NOT NULL,
    as_of            date    NOT NULL,
    beta             numeric(14, 8),
    t_stat           numeric(14, 8),
    significance     text,
    organization_id  uuid,
    computed_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ux_fund_factor_exposures_pk
        UNIQUE NULLS NOT DISTINCT (instrument_id, factor, as_of, organization_id)
);

CREATE INDEX IF NOT EXISTS fund_factor_exposures_iid_idx
    ON fund_factor_exposures (instrument_id, as_of DESC);
