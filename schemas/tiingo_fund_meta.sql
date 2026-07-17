-- tiingo_fund_meta worker — Tiingo end-of-day metadata for the fund catalog.
--
-- Source: GET https://api.tiingo.com/tiingo/daily/{ticker}, which returns a
-- single JSON object {ticker, name, description, startDate, endDate,
-- exchangeCode} per ticker. The Investintell-Light fund dossier needs (a) a
-- per-fund descriptive paragraph (``description``) and (b) an inception date
-- (``startDate``); nothing persisted this today.
--
-- SCOPE: this table is PURELY the Tiingo descriptive prose + startDate. The
-- legacy allocation repo deliberately sources fund *attributes* (category,
-- objective, holdings, etc.) from SEC filings — that decision stands. Do NOT
-- widen this table into a general fund-attribute store; it is a thin descriptive
-- cache keyed by ticker.
--
-- Apply against the cloud with:
--   psql "$DATABASE_URL" -f schemas/tiingo_fund_meta.sql
-- The worker also self-bootstraps this DDL via ensure_schema() (idempotent),
-- so a fresh environment does not need a manual apply before the first run.

CREATE TABLE IF NOT EXISTS tiingo_fund_meta (
    ticker        text        PRIMARY KEY,   -- upper-cased canonical ticker
    name          text,
    description    text,                      -- long descriptive paragraph (the dossier prose)
    exchange_code text,
    start_date    date,                       -- Tiingo startDate → fund/ticker inception
    end_date      date,                       -- Tiingo endDate (last EOD bar; NULL if active/unknown)
    fetched_at    timestamptz NOT NULL DEFAULT now(),
    source_status text                        -- 'ok' when Tiingo returned metadata, 'not_found' on 404
);

-- Case-insensitive ticker lookups from the Light API / enrichment joins.
CREATE INDEX IF NOT EXISTS tiingo_fund_meta_ticker_idx
    ON tiingo_fund_meta (upper(ticker));

-- Staleness scans (skip-when-fresh) order by fetched_at.
CREATE INDEX IF NOT EXISTS tiingo_fund_meta_fetched_at_idx
    ON tiingo_fund_meta (fetched_at);
