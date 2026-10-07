-- Governed, owner-applied migration. Never run from a worker's ensure_schema.
-- TimescaleDB refresh_continuous_aggregate commits internally: it cannot be
-- called from a SECURITY DEFINER function/procedure. This capability only moves
-- the existing postgres-owned policy's next_start forward. That owner job does
-- the refresh in its own top-level transaction context.
-- Apply as postgres with ON_ERROR_STOP; see docs/runbooks/nport-series-profile-refresh.md.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';

DO $$
BEGIN
    IF current_user <> 'postgres' THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
            MESSAGE = 'nport_cagg_refresh_install_requires_postgres';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer') THEN
        RAISE EXCEPTION 'nport_cagg_refresh_worker_role_missing';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.request_nport_series_profile_refresh()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '2s'
AS $$
DECLARE
    target_mat_id integer;
    target_job record;
    policy_count integer;
    request_lock_acquired boolean;
    requested_at timestamptz := pg_catalog.clock_timestamp();
    alter_job_oid oid := pg_catalog.to_regprocedure(
        'public.alter_job(integer,interval,interval,integer,interval,boolean,jsonb,'
        'timestamp with time zone,boolean,regproc,boolean,timestamp with time zone,text,text)'
    );
BEGIN
    -- Resolve only the fixed, postgres-owned aggregate over the expected source.
    -- No relation name, range, force flag or job configuration is caller input.
    SELECT ca.mat_hypertable_id INTO target_mat_id
    FROM _timescaledb_catalog.continuous_agg AS ca
    JOIN _timescaledb_catalog.hypertable AS raw ON raw.id = ca.raw_hypertable_id
    JOIN pg_catalog.pg_class AS c ON c.oid =
        pg_catalog.to_regclass('public.cagg_nport_series_profile')
    JOIN pg_catalog.pg_roles AS owner_role ON owner_role.oid = c.relowner
    WHERE ca.user_view_schema = 'public'
      AND ca.user_view_name = 'cagg_nport_series_profile'
      AND raw.schema_name = 'public' AND raw.table_name = 'sec_nport_holdings'
      AND owner_role.rolname = 'postgres';
    IF target_mat_id IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = 'nport_cagg_refresh_target_missing_or_owner_changed';
    END IF;

    -- Calling every argument with its exact type avoids untrusted overloads in
    -- public. The exact routine must still be a Timescale extension member.
    IF alter_job_oid IS NULL OR NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_depend AS d
        JOIN pg_catalog.pg_extension AS e ON e.oid = d.refobjid
        JOIN pg_catalog.pg_proc AS p ON p.oid = d.objid
        JOIN pg_catalog.pg_roles AS owner_role ON owner_role.oid = p.proowner
        WHERE d.classid = 'pg_catalog.pg_proc'::regclass
          AND d.refclassid = 'pg_catalog.pg_extension'::regclass
          AND d.objid = alter_job_oid AND d.deptype = 'e'
          AND e.extname = 'timescaledb' AND owner_role.rolname = 'postgres'
          AND p.prokind = 'f' AND NOT p.prosecdef
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = 'nport_cagg_refresh_alter_job_contract_changed';
    END IF;

    SELECT count(*) INTO policy_count
    FROM timescaledb_information.jobs AS j
    WHERE j.hypertable_schema = 'public'
      AND j.hypertable_name = 'cagg_nport_series_profile'
      AND j.proc_schema = '_timescaledb_functions'
      AND j.proc_name = 'policy_refresh_continuous_aggregate';
    IF policy_count <> 1 THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = 'nport_cagg_refresh_requires_one_owner_policy';
    END IF;

    request_lock_acquired := pg_catalog.pg_try_advisory_xact_lock(900365::bigint);
    SELECT j.*, s.job_status, s.last_run_started_at INTO target_job
    FROM timescaledb_information.jobs AS j
    LEFT JOIN timescaledb_information.job_stats AS s ON s.job_id = j.job_id
    WHERE j.hypertable_schema = 'public'
      AND j.hypertable_name = 'cagg_nport_series_profile'
      AND j.proc_schema = '_timescaledb_functions'
      AND j.proc_name = 'policy_refresh_continuous_aggregate';
    IF target_job.owner::text <> 'postgres' OR NOT target_job.scheduled
       OR target_job.schedule_interval <> interval '6 hours'
       OR target_job.config IS DISTINCT FROM pg_catalog.jsonb_build_object(
           'mat_hypertable_id', target_mat_id,
           'start_offset', NULL,
           'end_offset', '1 day'
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = 'nport_cagg_refresh_policy_contract_changed';
    END IF;

    -- Serialize requests, never delay an already due/running owner job, and cap
    -- expedited retries at one per 15 minutes even if a caller repeatedly asks.
    -- The policy keeps its original interval and invalidation-based refresh.
    IF NOT request_lock_acquired
       OR target_job.job_status = 'Running'
       OR target_job.next_start <= requested_at
       OR target_job.last_run_started_at >= requested_at - interval '15 minutes' THEN
        RETURN target_job.job_id;
    END IF;
    PERFORM public.alter_job(
        target_job.job_id,
        NULL::interval, NULL::interval, NULL::integer, NULL::interval,
        NULL::boolean, NULL::jsonb, requested_at, false::boolean,
        NULL::regproc, NULL::boolean, NULL::timestamptz, NULL::text, NULL::text
    );
    RETURN target_job.job_id;
END;
$$;

ALTER FUNCTION public.request_nport_series_profile_refresh() OWNER TO postgres;
REVOKE ALL ON FUNCTION public.request_nport_series_profile_refresh() FROM PUBLIC;
-- CREATE OR REPLACE retains ACLs. Remove prior extra grants on a re-application.
DO $$
DECLARE
    grantee_name name;
BEGIN
    FOR grantee_name IN
        SELECT DISTINCT r.rolname
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))
        ) AS a
        JOIN pg_catalog.pg_roles AS r ON r.oid = a.grantee
        WHERE p.oid = 'public.request_nport_series_profile_refresh()'::regprocedure
          AND r.rolname <> 'postgres'
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL ON FUNCTION public.request_nport_series_profile_refresh() FROM %I',
            grantee_name
        );
    END LOOP;
END;
$$;
GRANT EXECUTE ON FUNCTION public.request_nport_series_profile_refresh() TO worker_writer;
COMMENT ON FUNCTION public.request_nport_series_profile_refresh() IS
    'Only expedites the existing postgres-owned 6h N-PORT profile refresh policy; '
    'no caller-controlled target/range/config; requests do not prove completion. '
    'Expedited retries limited to one per 15 minutes.';
COMMIT;
