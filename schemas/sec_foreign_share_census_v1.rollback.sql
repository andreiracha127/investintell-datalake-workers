-- Owner-applied rollback of census objects only. Export version history first
-- if it must survive rollback/reload; W1 and W1c objects are untouched.
BEGIN;
SET LOCAL lock_timeout = '5s';
SELECT pg_catalog.pg_advisory_xact_lock(79311, 173);
DROP FUNCTION IF EXISTS public.sec_foreign_share_census_at(bigint, date);
DROP TABLE IF EXISTS public.sec_foreign_share_census;
DROP TABLE IF EXISTS public.sec_foreign_share_census_sources;
COMMIT;
