-- Bond default events: one-time ADMIN prerequisite (run as a superuser, e.g. `postgres`,
-- on database `market`), BEFORE the worker's `install-schema` mode runs as worker_writer.
--
-- Scope: creates the four NOLOGIN group roles the pinned bond_credit SQL files and the
-- diagnostic release file expect, grants them USAGE on schema public, and grants exactly
-- two memberships:
--   * bond_credit_writer            -> worker_writer  (producer / installer)
--   * bond_default_diagnostic_reader -> app_runtime    (Light API: ONE atomic reader only)
-- app_runtime must NOT receive bond_credit_reader / bond_credit_auditor / bond_credit_writer.
--
-- It never alters existing roles: an existing role with LOGIN, SUPERUSER, CREATEROLE,
-- CREATEDB, REPLICATION or BYPASSRLS, one that is itself a member of any role, or one that
-- already has any member other than its intended principal (worker_writer for
-- bond_credit_writer, app_runtime for bond_default_diagnostic_reader, nobody for
-- bond_credit_reader / bond_credit_auditor) stops the transaction for manual reconciliation.
-- The exact membership sets are re-checked AFTER the grants and abort the transaction on
-- violation. It changes no default privileges and no ACL outside these four roles. Idempotent.
--
-- Run with psql only (`\set` is a psql meta-command), e.g. inside the database container:
--   psql -X -v ON_ERROR_STOP=1 -U postgres -d market -f admin_roles_v1.sql

\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    r text;
BEGIN
    IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user) THEN
        RAISE EXCEPTION 'bond_default_events admin roles: must run as a superuser (current_user=%)', current_user;
    END IF;
    IF current_database() <> 'market' THEN
        RAISE EXCEPTION 'bond_default_events admin roles: expected database market, got %', current_database();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'worker_writer')
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'app_runtime') THEN
        RAISE EXCEPTION 'bond_default_events admin roles: worker_writer and app_runtime must already exist';
    END IF;
    FOREACH r IN ARRAY ARRAY['bond_credit_reader', 'bond_credit_writer', 'bond_credit_auditor',
                             'bond_default_diagnostic_reader'] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                   WHERE rolname = r
                     AND (rolcanlogin OR rolsuper OR rolcreaterole OR rolcreatedb
                          OR rolreplication OR rolbypassrls)) THEN
            RAISE EXCEPTION 'bond_default_events admin roles: existing role % has unexpected attributes', r;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
                   JOIN pg_catalog.pg_roles g ON g.oid = m.member
                   WHERE g.rolname = r) THEN
            RAISE EXCEPTION 'bond_default_events admin roles: existing role % is a member of another role', r;
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m
                   JOIN pg_catalog.pg_roles g ON g.oid = m.roleid
                   JOIN pg_catalog.pg_roles u ON u.oid = m.member
                   WHERE g.rolname = r
                     AND u.rolname <> ALL (CASE r
                                             WHEN 'bond_credit_writer' THEN ARRAY['worker_writer']
                                             WHEN 'bond_default_diagnostic_reader' THEN ARRAY['app_runtime']
                                             ELSE ARRAY[]::text[]
                                           END)) THEN
            RAISE EXCEPTION 'bond_default_events admin roles: existing role % has an unexpected member', r;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = r) THEN
            EXECUTE pg_catalog.format('CREATE ROLE %I NOLOGIN', r);
        END IF;
        EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA public TO %I', r);
    END LOOP;
    IF pg_catalog.pg_has_role('app_runtime', 'bond_credit_reader', 'MEMBER')
       OR pg_catalog.pg_has_role('app_runtime', 'bond_credit_auditor', 'MEMBER')
       OR pg_catalog.pg_has_role('app_runtime', 'bond_credit_writer', 'MEMBER') THEN
        RAISE EXCEPTION 'bond_default_events admin roles: app_runtime already inherits a broad bond_credit role; reconcile first';
    END IF;
END $$;

GRANT bond_credit_writer TO worker_writer;
GRANT bond_default_diagnostic_reader TO app_runtime;

-- Post-grant invariants (abort, not just report).
DO $$
BEGIN
    IF NOT pg_catalog.pg_has_role('worker_writer', 'bond_credit_writer', 'MEMBER')
       OR NOT pg_catalog.pg_has_role('app_runtime', 'bond_default_diagnostic_reader', 'MEMBER') THEN
        RAISE EXCEPTION 'bond_default_events admin roles: intended memberships are missing after the grants';
    END IF;
    IF pg_catalog.pg_has_role('app_runtime', 'bond_credit_reader', 'MEMBER')
       OR pg_catalog.pg_has_role('app_runtime', 'bond_credit_auditor', 'MEMBER')
       OR pg_catalog.pg_has_role('app_runtime', 'bond_credit_writer', 'MEMBER') THEN
        RAISE EXCEPTION 'bond_default_events admin roles: app_runtime inherits a broad bond_credit role after the grants';
    END IF;
    IF NOT pg_catalog.has_schema_privilege('worker_writer', 'public', 'CREATE') THEN
        RAISE EXCEPTION 'bond_default_events admin roles: worker_writer lacks CREATE on schema public';
    END IF;
    -- Exact membership sets of the four group roles.
    IF EXISTS (
        SELECT g.rolname, array_agg(DISTINCT u.rolname ORDER BY u.rolname) AS members
        FROM pg_catalog.pg_roles g
        LEFT JOIN pg_catalog.pg_auth_members m ON m.roleid = g.oid
        LEFT JOIN pg_catalog.pg_roles u ON u.oid = m.member
        WHERE g.rolname IN ('bond_credit_reader', 'bond_credit_writer', 'bond_credit_auditor',
                            'bond_default_diagnostic_reader')
        GROUP BY g.rolname
        HAVING array_remove(array_agg(DISTINCT u.rolname ORDER BY u.rolname), NULL)
               IS DISTINCT FROM (CASE g.rolname
                                   WHEN 'bond_credit_writer' THEN ARRAY['worker_writer']::name[]
                                   WHEN 'bond_default_diagnostic_reader' THEN ARRAY['app_runtime']::name[]
                                   ELSE ARRAY[]::name[]
                                 END)
    ) THEN
        RAISE EXCEPTION 'bond_default_events admin roles: group role membership differs from the intended set after the grants';
    END IF;
END $$;

-- Read-back (must show: four NOLOGIN roles; worker_writer member of bond_credit_writer;
-- app_runtime member of bond_default_diagnostic_reader only).
SELECT rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb
FROM pg_catalog.pg_roles
WHERE rolname IN ('bond_credit_reader', 'bond_credit_writer', 'bond_credit_auditor',
                  'bond_default_diagnostic_reader')
ORDER BY rolname;
SELECT pg_catalog.pg_has_role('worker_writer', 'bond_credit_writer', 'MEMBER')               AS writer_ok,
       pg_catalog.pg_has_role('app_runtime', 'bond_default_diagnostic_reader', 'MEMBER')     AS api_reader_ok,
       pg_catalog.pg_has_role('app_runtime', 'bond_credit_reader', 'MEMBER')                 AS api_broad_reader_must_be_false,
       pg_catalog.pg_has_role('app_runtime', 'bond_credit_auditor', 'MEMBER')                AS api_auditor_must_be_false,
       pg_catalog.pg_has_role('app_runtime', 'bond_credit_writer', 'MEMBER')                 AS api_writer_must_be_false,
       pg_catalog.has_schema_privilege('worker_writer', 'public', 'CREATE')                  AS writer_can_create_in_public;

COMMIT;
