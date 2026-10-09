-- Owner-applied rollback of insider evidence only; W1 cover contracts remain.
BEGIN;
SET LOCAL lock_timeout = '5s';

DROP FUNCTION IF EXISTS sec_insider_ticker_issuer_at(text, date);
DROP TABLE IF EXISTS sec_insider_package_facts;
DROP TABLE IF EXISTS sec_insider_package_members;
DROP TABLE IF EXISTS sec_insider_packages;
DROP TABLE IF EXISTS sec_insider_filings;
DROP FUNCTION IF EXISTS sec_insider_symbol_keys(text[]);

COMMIT;
