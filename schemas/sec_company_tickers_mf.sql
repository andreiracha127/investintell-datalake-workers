-- sec_company_tickers_mf worker — SEC fund ticker <-> series crosswalk.
--
-- Source: https://www.sec.gov/files/company_tickers_mf.json, the SEC's
-- authoritative columnar map of (cik, seriesId, classId, symbol) for every
-- registered fund share class (mutual funds AND ETFs). This is the missing
-- ticker -> series_id edge for the look-through fund-of-fund resolver: held
-- ETFs like WisdomTree's DTD/DEM/DXJ are present in N-PORT but absent from the
-- N-CEN-derived sec_etfs catalog, so their CUSIP (resolved to a ticker via
-- sec_cusip_ticker_map) had no series_id to expand into.
--
-- Apply against the cloud with:
--   psql "$DATABASE_URL" -f schemas/sec_company_tickers_mf.sql

CREATE TABLE IF NOT EXISTS sec_company_tickers_mf (
    class_id    text        NOT NULL PRIMARY KEY,  -- C000... globally unique
    cik         text        NOT NULL,
    series_id   text        NOT NULL,
    ticker      text        NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- The resolver edge: upper(ticker) -> series_id (matches the t2s join).
CREATE INDEX IF NOT EXISTS sec_company_tickers_mf_ticker_idx
    ON sec_company_tickers_mf (upper(ticker));

CREATE INDEX IF NOT EXISTS sec_company_tickers_mf_series_idx
    ON sec_company_tickers_mf (series_id);
