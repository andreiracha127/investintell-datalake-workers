-- Exact classification inputs derived from uncollapsed DERA N-PORT holdings.
CREATE TABLE IF NOT EXISTS nport_equity_exposure_summary (
    report_date       date          NOT NULL,
    series_id         text          NOT NULL,
    gross_equity_pct  numeric(14,6) NOT NULL,
    net_equity_pct    numeric(14,6) NOT NULL,
    source_quarter    text          NOT NULL,
    computed_at       timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (report_date, series_id)
);

CREATE INDEX IF NOT EXISTS nport_equity_exposure_summary_series_idx
    ON nport_equity_exposure_summary (series_id, report_date DESC);

CREATE TABLE IF NOT EXISTS nport_equity_country_exposures (
    report_date       date          NOT NULL,
    series_id         text          NOT NULL,
    country           text          NOT NULL,
    direct_pct        numeric(14,6) NOT NULL,
    source_quarter    text          NOT NULL,
    computed_at       timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (report_date, series_id, country)
);

CREATE INDEX IF NOT EXISTS nport_equity_country_exposures_series_idx
    ON nport_equity_country_exposures (series_id, report_date DESC);
