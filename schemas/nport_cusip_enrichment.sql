-- sec_isin_sector — ISIN → GICS sector for international (non-US) equities.
-- Enriched by the nport_cusip_enrichment worker (OpenFIGI ISIN→ticker +
-- yfinance ticker→sector). Complements the US-centric sec_cusip_ticker_map;
-- the nport_lookthrough worker reads BOTH (CUSIP-6 first, then ISIN). Misses
-- are cached too (gics_sector NULL + a reason in resolved_via) so unresolved
-- names are not re-queried every run; last_verified_at drives the TTL refresh.
CREATE TABLE IF NOT EXISTS sec_isin_sector (
    isin             text PRIMARY KEY,
    gics_sector      text,
    ticker           text,
    yahoo_symbol     text,
    resolved_via     text,
    last_verified_at timestamptz NOT NULL DEFAULT now()
);
