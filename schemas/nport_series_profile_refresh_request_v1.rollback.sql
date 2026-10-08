-- Governed rollback, as postgres. Existing policy, ownership and data survive.
BEGIN;
SET LOCAL lock_timeout = '2s';
DO $$
BEGIN
    IF current_user <> 'postgres' THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
            MESSAGE = 'nport_cagg_refresh_rollback_requires_postgres';
    END IF;
END;
$$;
DROP FUNCTION IF EXISTS public.request_nport_series_profile_refresh();
COMMIT;
