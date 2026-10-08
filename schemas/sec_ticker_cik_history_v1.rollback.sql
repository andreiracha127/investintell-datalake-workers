-- Governed rollback of schemas/sec_ticker_cik_history_v1.sql (owner-applied,
-- psql with ON_ERROR_STOP). Drops the resolvers and the view, then the loaded
-- history; the history is reproducible from the public SEC sources by the loader.
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP FUNCTION IF EXISTS sec_ticker_price_span(text, bigint, text);
DROP FUNCTION IF EXISTS sec_cover_class_shares_at(bigint, text, date, integer);
DROP FUNCTION IF EXISTS sec_issuer_line_at(bigint, text, date, integer);
DROP FUNCTION IF EXISTS sec_ticker_issuer_at(text, date, integer);
DROP FUNCTION IF EXISTS sec_ticker_lines_at(text, date, integer);
DROP VIEW IF EXISTS sec_ticker_intervals;
DROP TABLE IF EXISTS sec_ticker_cik_observations;
DROP TABLE IF EXISTS sec_cover_share_counts;
DROP TABLE IF EXISTS sec_registration_events;
DROP TABLE IF EXISTS sec_ticker_cik_packages;
COMMIT;
