# N-PORT profile refresh capability

`cagg_nport_series_profile` is a postgres-owned, materialized-only Timescale
continuous aggregate over `public.sec_nport_holdings`. It groups `series_id` and
one-day `report_date` buckets; `report_day` is a date, and `n_holdings` is the raw
holding count in that bucket. Workers read its holdings coverage before publishing
lookthrough and fund classification.

The production catalog read on 2026-10-07 confirmed Timescale 2.27.2 and one healthy
postgres-owned refresh policy, job 1078, every six hours. The policy uses
`start_offset = NULL` and `end_offset = '1 day'`. Its last inspected refresh took
8.43 seconds. The migration resolves this job through the aggregate identity; it
does not hard-code the environment-specific job id.

## Why the function schedules the owner job

Granting `EXECUTE` on `refresh_continuous_aggregate` does not satisfy its aggregate
owner check. A direct `SECURITY DEFINER` refresh wrapper also fails: Timescale
rejects refresh inside a function and commits between refresh phases. PostgreSQL
forbids transaction control in definer procedures or procedures with a `SET`
clause. The [Timescale 2.27.2 implementation](https://github.com/timescale/timescaledb/blob/2.27.2/tsl/src/continuous_aggs/refresh.c)
and [PostgreSQL procedure rules](https://www.postgresql.org/docs/current/sql-createprocedure.html)
describe those restrictions. The disposable database test exercises both failures.

`SELECT public.request_nport_series_profile_refresh()` returns the existing policy
job id. It advances only that policy's `next_start` through `public.alter_job`.
The existing postgres-owned scheduler performs the refresh in its own top-level
context. A returned job id is a request receipt; it never certifies completed or
fresh output. The worker must poll its source/profile alignment check with its
bounded timeout, exit non-zero while pending, and preserve the last good output.
Commit source writes before requesting, then commit the request transaction
before polling. An uncommitted `next_start` change is invisible to the scheduler.

## Permission and load limits

- The function has no arguments. Callers cannot choose another relation, date
  range, policy, configuration, or `force` flag.
- Owner is `postgres`; `EXECUTE` is granted only to `worker_writer` and the owner.
  Reapplying the migration removes stale extra ACL grants. Workers do not install
  or replace this function in `ensure_schema`.
- Its search path is `pg_catalog, pg_temp`; relation references are qualified.
  It checks that the exact, fully typed `alter_job` routine remains part of the
  Timescale extension and owned by postgres.
- It requires the expected postgres-owned aggregate over the expected raw
  hypertable, exactly one owner policy, `scheduled = true`, a six-hour interval,
  and the inspected policy configuration. Missing, disabled, ambiguous, or
  changed policy state raises SQLSTATE `55000` before scheduling anything.
- A running or already due policy is left due. A start in the previous 15 minutes
  suppresses another expedited attempt. Concurrent requests use advisory
  transaction lock `900365`; lock waits are capped at two seconds. These checks
  prevent callers from turning retries into frequent heavy refreshes.
- The existing policy's invalidation processing and date window are preserved.
  A request introduces no all-history forced refresh. The existing unbounded
  `start_offset` can still process historical invalidations; investigate a long
  owner job through job stats before retrying heavy recovery work.

## Apply order

This session prepares the migration and does not apply it to production. An
authorized operator applies it before deploying the workers that call the new
function. Keep the postgres credential in the approved operator environment.

1. Read the live catalog as a read-only role:

   ```sql
   SELECT view_schema, view_name, view_owner, hypertable_schema, hypertable_name,
          materialized_only, view_definition
   FROM timescaledb_information.continuous_aggregates
   WHERE view_schema='public' AND view_name='cagg_nport_series_profile';

   SELECT j.job_id, j.owner::text, j.scheduled, j.schedule_interval, j.config,
          s.job_status, s.last_run_status, s.last_run_started_at,
          s.last_successful_finish, s.next_start
   FROM timescaledb_information.jobs j
   LEFT JOIN timescaledb_information.job_stats s USING (job_id)
   WHERE j.hypertable_schema='public'
     AND j.hypertable_name='cagg_nport_series_profile';
   ```

2. As postgres, apply the reviewed owner migration with errors fatal:

   ```sh
   psql "$DATALAKE_ADMIN_DSN" -X -v ON_ERROR_STOP=1 \
     -f schemas/nport_series_profile_refresh_request_v1.sql
   ```

   The migration only installs the capability. It does not call it or start a
   refresh. It is one transaction with a 15-second statement limit and two-second
   lock limit; it must be applied outside a worker's schema bootstrap.

3. Verify ownership and ACLs before deploying workers:

   ```sql
   SELECT p.oid::regprocedure, r.rolname, p.prosecdef, p.proconfig, p.proacl
   FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_roles r ON r.oid=p.proowner
   WHERE p.oid='public.request_nport_series_profile_refresh()'::regprocedure;

   SELECT has_function_privilege('worker_writer',
     'public.request_nport_series_profile_refresh()', 'EXECUTE');
   ```

4. Deploy the authorized workers revision, then the linked Light revision in the
   cross-repository release order. The first stale-profile worker attempt requests
   the existing owner job and verifies actual alignment. Compare that policy's
   recorded successful finish and profile/source cohort coverage; an advanced
   `next_start` alone is insufficient completion evidence.

## Rollback

First restore a worker revision that does not require this capability, then apply
`schemas/nport_series_profile_refresh_request_v1.rollback.sql` as postgres with
`ON_ERROR_STOP=1`. It drops only the request function, without cascading. The
aggregate, data, existing policy, and its normal schedule remain. A request
already accepted may still run; dropping the capability does not cancel it.

## Focused local validation

```powershell
$env:NPORT_CAGG_REFRESH_DB_TEST = '1'
uv run --no-project --with pytest python -m pytest -q tests/test_nport_series_profile_refresh_request_db.py
Remove-Item Env:\NPORT_CAGG_REFRESH_DB_TEST
```

The test starts a fresh `timescale/timescaledb:2.27.2-pg18` container from the local
image cache, with no networking, host ports, volumes, or credential lookup. Every
case uses a disposable database. It proves owner-policy materialization,
permission denial, no arbitrary relation/range, overload and temporary-relation
resistance, changed-policy rejection, due/recent-run idempotence, ACL repair, and
rollback. It does not measure the production backfill's IO or change production.
