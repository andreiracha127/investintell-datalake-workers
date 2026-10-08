-- Governed rollback of schemas/sec_ticker_cik_history_v1.sql (owner-applied,
-- psql with ON_ERROR_STOP). Drops the resolver, then the loaded history; the
-- history is reproducible from the public DERA packages by the loader.
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP FUNCTION IF EXISTS sec_ticker_issuer_at(text, date, integer);
DROP TABLE IF EXISTS sec_ticker_cik_observations;
DROP TABLE IF EXISTS sec_ticker_cik_packages;
COMMIT;
