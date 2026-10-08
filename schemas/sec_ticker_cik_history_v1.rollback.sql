-- Governed rollback of schemas/sec_ticker_cik_history_v1.sql (owner-applied,
-- psql with ON_ERROR_STOP). Drops the functions and the view, then the loaded
-- history; the history is reproducible from the public SEC sources by the loader
-- (from the reload date on: a reload is a new first load).
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP FUNCTION IF EXISTS sec_ticker_price_span(text, bigint, text);
DROP FUNCTION IF EXISTS sec_cover_ticker_shares_at(text, bigint, date, integer);
DROP FUNCTION IF EXISTS sec_cover_class_shares_at(bigint, text, date, integer);
DROP FUNCTION IF EXISTS sec_issuer_line_at(bigint, text, date, integer);
DROP FUNCTION IF EXISTS sec_ticker_issuer_at(text, date, integer);
DROP FUNCTION IF EXISTS sec_ticker_holds(text, date, integer, boolean);
DROP FUNCTION IF EXISTS sec_issuer_end_events(bigint, date, boolean);
DROP FUNCTION IF EXISTS sec_registration_starts(bigint, date, boolean);
DROP FUNCTION IF EXISTS sec_registration_end_events(bigint, date, boolean);
DROP FUNCTION IF EXISTS sec_share_counts_at(date, boolean);
DROP FUNCTION IF EXISTS sec_observations_at(date, boolean);
DROP VIEW IF EXISTS sec_ticker_intervals;
DROP TABLE IF EXISTS sec_ticker_cik_observations;
DROP TABLE IF EXISTS sec_cover_share_counts;
DROP TABLE IF EXISTS sec_registration_events;
DROP TABLE IF EXISTS sec_ticker_cik_packages;
DROP TABLE IF EXISTS sec_ticker_cik_package_members;
DROP TABLE IF EXISTS sec_ticker_cik_package_facts;
COMMIT;
