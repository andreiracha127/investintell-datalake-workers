-- Owner-applied rollback. Removes W1c evidence only; no W1 objects are touched.
-- Export the version history first if it must survive a rollback/reload.
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP FUNCTION IF EXISTS public.sec_foreign_listing_at(bigint, text, date);
DROP TABLE IF EXISTS public.sec_foreign_listing_evidence;
DROP TABLE IF EXISTS public.sec_foreign_listing_sources;
COMMIT;
