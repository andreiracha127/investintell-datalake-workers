-- Transactional production migration for fit-versioned IPCA fund exposures.
-- Existing readers either see the old MV or the fully rebuilt replacement.
BEGIN;

ALTER TABLE fund_factor_exposures
    DROP CONSTRAINT IF EXISTS ux_fund_factor_exposures_pk;
ALTER TABLE fund_factor_exposures
    ADD CONSTRAINT ux_fund_factor_exposures_pk
    UNIQUE NULLS NOT DISTINCT (
        instrument_id, factor, as_of, organization_id, fit_id
    );

DROP MATERIALIZED VIEW IF EXISTS fund_factor_exposures_latest_mv;

CREATE MATERIALIZED VIEW fund_factor_exposures_latest_mv AS
WITH latest_materialized_fit AS (
    SELECT exposures.fit_id
    FROM fund_factor_exposures exposures
    JOIN factor_model_fits fits ON fits.fit_id = exposures.fit_id
    WHERE exposures.organization_id IS NULL
      AND fits.engine = 'ipca'
      AND fits.asset_class = 'Equity'
      AND fits.converged IS TRUE
      AND fits.degraded IS FALSE
      AND fits.production_fit IS TRUE
    GROUP BY exposures.fit_id, fits.fit_date, fits.created_at
    ORDER BY fits.fit_date DESC, fits.created_at DESC
    LIMIT 1
)
SELECT exposures.instrument_id,
       exposures.factor,
       exposures.factor_index,
       exposures.beta,
       exposures.t_stat,
       exposures.significance,
       exposures.as_of,
       exposures.fit_id,
       exposures.n_observations AS exposure_n_observations,
       exposures.r_squared,
       fits.fit_date,
       fits.k_factors,
       fits.in_sample_r_squared,
       fits.oos_r_squared,
       fits.converged,
       fits.n_iterations,
       fits.sample_start,
       fits.sample_end,
       fits.n_observations AS fit_n_observations,
       fits.feature_names,
       fits.selection_metadata,
       fits.degraded,
       fits.degraded_reason
FROM fund_factor_exposures exposures
JOIN latest_materialized_fit latest ON latest.fit_id = exposures.fit_id
JOIN factor_model_fits fits ON fits.fit_id = exposures.fit_id
WHERE exposures.organization_id IS NULL;

CREATE UNIQUE INDEX fund_factor_exposures_latest_mv_pk
    ON fund_factor_exposures_latest_mv (instrument_id, factor);

GRANT MAINTAIN ON TABLE fund_factor_exposures_latest_mv TO worker_writer;

COMMIT;
