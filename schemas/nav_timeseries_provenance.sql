-- Transactional additive upgrade for nav_timeseries provenance columns.
-- The operator must select exactly one explicit target schema in search_path.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';
SET LOCAL idle_in_transaction_session_timeout = '20s';

DO $nav_provenance$
DECLARE
    target_search_path text := current_setting('search_path');
    target_schema text;
    target_namespace oid;
    target_relation oid;
    locked_relation oid;
    expected record;
    observed record;
BEGIN
    IF pg_catalog.strpos(target_search_path, ',') > 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '3F000',
            MESSAGE = 'nav_timeseries upgrade requires exactly one explicit target schema';
    END IF;

    target_schema := pg_catalog.btrim(pg_catalog.btrim(target_search_path), '"');

    IF target_schema !~ '^[A-Za-z_][A-Za-z0-9_$]{0,62}$'
       OR current_schema() IS DISTINCT FROM target_schema THEN
        RAISE EXCEPTION USING
            ERRCODE = '3F000',
            MESSAGE = 'nav_timeseries target schema is not available';
    END IF;

    SELECT n.oid
      INTO target_namespace
      FROM pg_catalog.pg_namespace AS n
     WHERE n.nspname = target_schema;

    IF target_namespace IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '3F000',
            MESSAGE = 'nav_timeseries target schema is not available';
    END IF;

    SELECT pg_catalog.to_regclass(
        pg_catalog.format('%I.%I', target_schema, 'nav_timeseries')
    )
      INTO target_relation;

    IF target_relation IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '42P01',
            MESSAGE = 'nav_timeseries target table does not exist in selected schema';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_class AS c
         WHERE c.oid = target_relation
           AND c.relnamespace = target_namespace
           AND c.relkind = 'r'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '42809',
            MESSAGE = 'nav_timeseries target relation is not the expected table';
    END IF;

    EXECUTE pg_catalog.format(
        'LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',
        target_schema,
        'nav_timeseries'
    );

    SELECT pg_catalog.to_regclass(
        pg_catalog.format('%I.%I', target_schema, 'nav_timeseries')
    )
      INTO locked_relation;

    IF locked_relation IS DISTINCT FROM target_relation THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'nav_timeseries target relation changed while acquiring lock';
    END IF;

    FOR expected IN
        SELECT *
          FROM (VALUES
              ('source_nav', 'numeric(18,6)'),
              ('source_nav_kind', 'character varying(16)'),
              ('nav_repair_kind', 'character varying(48)'),
              ('return_start_date', 'date'),
              ('return_source_boundary', 'boolean'),
              ('return_uses_repaired_nav', 'boolean'),
              ('return_semantics', 'character varying(48)'),
              ('return_verification_status', 'character varying(24)'),
              ('calendar_id', 'character varying(128)'),
              ('calendar_version', 'character varying(64)'),
              ('calendar_source', 'text')
          ) AS contract(column_name, type_name)
    LOOP
        SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
               a.attnotnull AS is_not_null,
               a.attgenerated AS generated_kind,
               a.attidentity AS identity_kind,
               ad.oid IS NOT NULL AS has_default
          INTO observed
          FROM pg_catalog.pg_attribute AS a
          LEFT JOIN pg_catalog.pg_attrdef AS ad
            ON ad.adrelid = a.attrelid
           AND ad.adnum = a.attnum
         WHERE a.attrelid = target_relation
           AND a.attname = expected.column_name
           AND a.attnum > 0
           AND NOT a.attisdropped;

        IF FOUND AND (
            observed.type_name <> expected.type_name
            OR observed.is_not_null
            OR observed.has_default
            OR observed.generated_kind <> ''
            OR observed.identity_kind <> ''
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '42804',
                MESSAGE = pg_catalog.format(
                    'nav_timeseries schema contract mismatch for column %s; expected %s nullable without default/generated/identity',
                    expected.column_name,
                    expected.type_name
                );
        END IF;
    END LOOP;

    EXECUTE pg_catalog.format(
        $alter$
        ALTER TABLE %I.%I
            ADD COLUMN IF NOT EXISTS source_nav NUMERIC(18,6),
            ADD COLUMN IF NOT EXISTS source_nav_kind VARCHAR(16),
            ADD COLUMN IF NOT EXISTS nav_repair_kind VARCHAR(48),
            ADD COLUMN IF NOT EXISTS return_start_date DATE,
            ADD COLUMN IF NOT EXISTS return_source_boundary BOOLEAN,
            ADD COLUMN IF NOT EXISTS return_uses_repaired_nav BOOLEAN,
            ADD COLUMN IF NOT EXISTS return_semantics VARCHAR(48),
            ADD COLUMN IF NOT EXISTS return_verification_status VARCHAR(24),
            ADD COLUMN IF NOT EXISTS calendar_id VARCHAR(128),
            ADD COLUMN IF NOT EXISTS calendar_version VARCHAR(64),
            ADD COLUMN IF NOT EXISTS calendar_source TEXT
        $alter$,
        target_schema,
        'nav_timeseries'
    );

    FOR expected IN
        SELECT *
          FROM (VALUES
              ('source_nav', 'numeric(18,6)'),
              ('source_nav_kind', 'character varying(16)'),
              ('nav_repair_kind', 'character varying(48)'),
              ('return_start_date', 'date'),
              ('return_source_boundary', 'boolean'),
              ('return_uses_repaired_nav', 'boolean'),
              ('return_semantics', 'character varying(48)'),
              ('return_verification_status', 'character varying(24)'),
              ('calendar_id', 'character varying(128)'),
              ('calendar_version', 'character varying(64)'),
              ('calendar_source', 'text')
          ) AS contract(column_name, type_name)
    LOOP
        SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name,
               a.attnotnull AS is_not_null,
               a.attgenerated AS generated_kind,
               a.attidentity AS identity_kind,
               ad.oid IS NOT NULL AS has_default
          INTO observed
          FROM pg_catalog.pg_attribute AS a
          LEFT JOIN pg_catalog.pg_attrdef AS ad
            ON ad.adrelid = a.attrelid
           AND ad.adnum = a.attnum
         WHERE a.attrelid = target_relation
           AND a.attname = expected.column_name
           AND a.attnum > 0
           AND NOT a.attisdropped;

        IF NOT FOUND OR (
            observed.type_name <> expected.type_name
            OR observed.is_not_null
            OR observed.has_default
            OR observed.generated_kind <> ''
            OR observed.identity_kind <> ''
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '42804',
                MESSAGE = pg_catalog.format(
                    'nav_timeseries post-upgrade contract mismatch for column %s; expected %s nullable without default/generated/identity',
                    expected.column_name,
                    expected.type_name
                );
        END IF;
    END LOOP;
END
$nav_provenance$;

COMMIT;
