-- Independent agency-free owner legal-event evidence. Additive PREPARATION ONLY.
-- Not the agency/public-PIT bond_credit_evidence_v1 product or the SEC ledger:
-- an app-DB review export has no validated SEC run/package lineage to invent.
-- Install only with separately authorized owner credentials, in one transaction.
-- Set search_path to the intended owner-controlled schema before applying this DDL.
-- Writers/readers must not own these objects, inherit the owner or CREATE in that schema.
-- Every function is bound below to pg_catalog, the installation schema, pg_temp (last).
DO $$
BEGIN
    IF current_schema() IS NULL OR current_schema() IN ('pg_catalog','information_schema')
        OR current_schema() LIKE 'pg_temp_%' THEN
        RAISE EXCEPTION 'owner evidence requires a permanent owner-controlled installation schema';
    END IF;
    -- Keep the creation target first, but suppress implicit temporary-table
    -- precedence during installation too. The caller's transaction restores it.
    PERFORM set_config('search_path',format('%I, pg_catalog, pg_temp',current_schema()),true);
END $$;

-- CHECK expressions must not accidentally admit JSON null via SQL UNKNOWN.
CREATE OR REPLACE FUNCTION bond_default_owner_evidence_v1_required_object(value jsonb, fields text[])
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT jsonb_typeof(value)='object' AND NOT EXISTS (
        SELECT 1 FROM unnest(fields) AS required(key)
        WHERE NOT (value ? key) OR value->key='null'::jsonb
    )
$$;

CREATE TABLE IF NOT EXISTS bond_default_owner_evidence_v1_builds (
    publication_id uuid PRIMARY KEY,
    policy_version text NOT NULL CHECK (policy_version = 'bond_default_owner_event_policy_v1'),
    policy_digest text NOT NULL CHECK (policy_digest ~ '^[0-9a-f]{64}$'),
    code_revision text NOT NULL CHECK (length(code_revision) > 0),
    code_digest text NOT NULL CHECK (code_digest ~ '^[0-9a-f]{64}$'),
    owner_sub text NOT NULL CHECK (owner_sub ~ '^user_[0-9A-Za-z]+$'),
    knowledge_cutoff timestamptz NOT NULL,
    export_sha256 text NOT NULL CHECK (export_sha256 ~ '^[0-9a-f]{64}$'),
    issuer_mapping_digest text NOT NULL CHECK (issuer_mapping_digest ~ '^[0-9a-f]{64}$'),
    events_sha256 text NOT NULL CHECK (events_sha256 ~ '^[0-9a-f]{64}$'),
    bundle_sha256 text NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    proposal_count bigint NOT NULL CHECK (proposal_count >= 0),
    decision_count bigint NOT NULL CHECK (decision_count >= 0),
    resolution_count bigint NOT NULL CHECK (resolution_count >= 0),
    accepted_event_count bigint NOT NULL CHECK (accepted_event_count >= 0),
    payload jsonb NOT NULL,
    prepared_at timestamptz NOT NULL DEFAULT now(),
    CHECK (bond_default_owner_evidence_v1_required_object(payload, ARRAY['schema_version','product',
        'economic_authority','lifecycle_state','publication_id','policy_version','policy_digest',
        'code_revision','code_digest','owner_sub','knowledge_cutoff','export_sha256',
        'issuer_mapping_digest','events_sha256','bundle_sha256','proposal_count','decision_count',
        'resolution_count','accepted_event_count','events','source_export'])),
    CHECK (payload->>'schema_version' = 'bond_default_owner_evidence_v1_bundle'),
    CHECK (payload->>'product' = 'bond_default_owner_evidence_v1'),
    CHECK (payload->'economic_authority' = 'false'::jsonb),
    CHECK (payload->>'lifecycle_state' = 'prepared'),
    CHECK ((payload->>'publication_id')::uuid = publication_id),
    CHECK (payload->>'policy_version' = policy_version AND payload->>'policy_digest' = policy_digest),
    CHECK (payload->>'code_revision' = code_revision AND payload->>'code_digest' = code_digest),
    CHECK (payload->>'owner_sub' = owner_sub),
    CHECK ((payload->>'knowledge_cutoff')::timestamptz = knowledge_cutoff),
    CHECK (payload->>'export_sha256' = export_sha256),
    CHECK (payload->>'issuer_mapping_digest' = issuer_mapping_digest),
    CHECK (payload->>'events_sha256' = events_sha256 AND payload->>'bundle_sha256' = bundle_sha256),
    CHECK ((payload->>'proposal_count')::bigint = proposal_count),
    CHECK ((payload->>'decision_count')::bigint = decision_count),
    CHECK ((payload->>'resolution_count')::bigint = resolution_count),
    CHECK ((payload->>'accepted_event_count')::bigint = accepted_event_count),
    CHECK (jsonb_array_length(payload->'events') = accepted_event_count)
);

CREATE TABLE IF NOT EXISTS bond_default_owner_evidence_v1_events (
    publication_id uuid NOT NULL REFERENCES bond_default_owner_evidence_v1_builds(publication_id),
    event_id uuid NOT NULL,
    decision_id uuid NOT NULL,
    proposal_id uuid NOT NULL,
    cusip9 text NOT NULL CHECK (cusip9 ~ '^[0-9A-Z*@#]{8}[0-9]$'),
    event_date date NOT NULL,
    event_type text NOT NULL CHECK (event_type IN
        ('chapter_11', 'chapter_7', 'missed_payment_after_cure', 'distressed_exchange')),
    link_sha256 text NOT NULL CHECK (link_sha256 ~ '^[0-9a-f]{64}$'),
    payload jsonb NOT NULL,
    PRIMARY KEY (publication_id,event_id),
    UNIQUE (publication_id,decision_id),
    CHECK (bond_default_owner_evidence_v1_required_object(payload, ARRAY['event_id','decision_id',
        'proposal_id','cusip9','event_date','event_type','link_sha256','economic_authority','issue_link'])),
    CHECK (bond_default_owner_evidence_v1_required_object(payload->'issue_link', ARRAY['basis','cusip9'])),
    CHECK ((payload->>'event_id')::uuid = event_id),
    CHECK ((payload->>'decision_id')::uuid = decision_id AND (payload->>'proposal_id')::uuid = proposal_id),
    CHECK (payload->>'cusip9' = cusip9 AND (payload->>'event_date')::date = event_date),
    CHECK (payload->>'event_type' = event_type AND payload->>'link_sha256' = link_sha256),
    CHECK (payload->'economic_authority' = 'false'::jsonb),
    CHECK (payload->'issue_link'->>'basis' = 'owner_adjudicated_issue_scope'),
    CHECK (payload->'issue_link'->>'cusip9' = cusip9)
);

-- Validation is an appended receipt, never a fabricated lifecycle update.
CREATE TABLE IF NOT EXISTS bond_default_owner_evidence_v1_validations (
    publication_id uuid PRIMARY KEY REFERENCES bond_default_owner_evidence_v1_builds(publication_id),
    bundle_sha256 text NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    events_sha256 text NOT NULL CHECK (events_sha256 ~ '^[0-9a-f]{64}$'),
    accepted_event_count bigint NOT NULL CHECK (accepted_event_count >= 0),
    receipt jsonb NOT NULL,
    validated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (bond_default_owner_evidence_v1_required_object(receipt, ARRAY['publication_id','bundle_sha256',
        'events_sha256','accepted_event_count','verification'])),
    CHECK ((receipt->>'publication_id')::uuid = publication_id),
    CHECK (receipt->>'bundle_sha256' = bundle_sha256 AND receipt->>'events_sha256' = events_sha256),
    CHECK ((receipt->>'accepted_event_count')::bigint = accepted_event_count),
    CHECK (receipt->>'verification' = 'replayed_export_and_stored_issue_rows')
);

CREATE TABLE IF NOT EXISTS bond_default_owner_evidence_v1_pointer (
    product text PRIMARY KEY CHECK (product = 'bond_default_owner_evidence_v1'),
    publication_id uuid NOT NULL REFERENCES bond_default_owner_evidence_v1_validations(publication_id),
    set_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bond_default_owner_evidence_v1_pointer_tokens (
    product text PRIMARY KEY,
    backend_pid integer NOT NULL
);

CREATE OR REPLACE FUNCTION bond_default_owner_evidence_v1_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'owner evidence is append-only; % is forbidden', TG_OP;
END $$;

DO $$
DECLARE relation text;
BEGIN
    FOREACH relation IN ARRAY ARRAY['bond_default_owner_evidence_v1_builds',
        'bond_default_owner_evidence_v1_events', 'bond_default_owner_evidence_v1_validations']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS owner_evidence_immutable ON %I', relation);
        EXECUTE format('CREATE TRIGGER owner_evidence_immutable BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION bond_default_owner_evidence_v1_immutable()', relation);
        EXECUTE format('DROP TRIGGER IF EXISTS owner_evidence_no_truncate ON %I', relation);
        EXECUTE format('CREATE TRIGGER owner_evidence_no_truncate BEFORE TRUNCATE ON %I '
            'FOR EACH STATEMENT EXECUTE FUNCTION bond_default_owner_evidence_v1_immutable()', relation);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION bond_default_owner_evidence_v1_event_insert_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM bond_default_owner_evidence_v1_builds
        WHERE publication_id=NEW.publication_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM bond_default_owner_evidence_v1_validations
        WHERE publication_id=NEW.publication_id) THEN
        RAISE EXCEPTION 'validated owner issue rows are sealed';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS owner_evidence_event_insert ON bond_default_owner_evidence_v1_events;
CREATE TRIGGER owner_evidence_event_insert BEFORE INSERT ON bond_default_owner_evidence_v1_events
    FOR EACH ROW EXECUTE FUNCTION bond_default_owner_evidence_v1_event_insert_guard();

CREATE OR REPLACE FUNCTION bond_default_owner_evidence_v1_validation_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE build bond_default_owner_evidence_v1_builds%ROWTYPE; stored_events jsonb; stored_count bigint;
BEGIN
    SELECT * INTO build FROM bond_default_owner_evidence_v1_builds
        WHERE publication_id=NEW.publication_id FOR UPDATE;
    SELECT count(*), COALESCE(jsonb_agg(payload ORDER BY event_id), '[]'::jsonb)
        INTO stored_count,stored_events FROM bond_default_owner_evidence_v1_events
        WHERE publication_id=NEW.publication_id;
    IF build.publication_id IS NULL OR stored_count<>build.accepted_event_count
        OR stored_count<>NEW.accepted_event_count OR stored_events IS DISTINCT FROM build.payload->'events'
        OR NEW.bundle_sha256<>build.bundle_sha256 OR NEW.events_sha256<>build.events_sha256 THEN
        RAISE EXCEPTION 'owner validation requires matching persisted count, rows and bundle pins';
    END IF;
    -- Python separately replays ALL export/hash/chain/link semantics before this receipt insert.
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS owner_evidence_validation_insert ON bond_default_owner_evidence_v1_validations;
CREATE TRIGGER owner_evidence_validation_insert BEFORE INSERT ON bond_default_owner_evidence_v1_validations
    FOR EACH ROW EXECUTE FUNCTION bond_default_owner_evidence_v1_validation_guard();

CREATE OR REPLACE FUNCTION bond_default_owner_evidence_v1_pointer_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM bond_default_owner_evidence_v1_pointer_tokens
        WHERE product=COALESCE(NEW.product,OLD.product) AND backend_pid=pg_backend_pid()) THEN
        RAISE EXCEPTION 'owner evidence pointer requires explicit compare-and-set';
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;
DROP TRIGGER IF EXISTS owner_evidence_pointer_guard ON bond_default_owner_evidence_v1_pointer;
CREATE TRIGGER owner_evidence_pointer_guard BEFORE INSERT OR UPDATE OR DELETE ON bond_default_owner_evidence_v1_pointer
    FOR EACH ROW EXECUTE FUNCTION bond_default_owner_evidence_v1_pointer_guard();
DROP TRIGGER IF EXISTS owner_evidence_pointer_no_truncate ON bond_default_owner_evidence_v1_pointer;
CREATE TRIGGER owner_evidence_pointer_no_truncate BEFORE TRUNCATE ON bond_default_owner_evidence_v1_pointer
    FOR EACH STATEMENT EXECUTE FUNCTION bond_default_owner_evidence_v1_immutable();

-- EXPLICIT operational authorization only. prepare/validate/CLI never call this.
CREATE OR REPLACE FUNCTION bond_default_owner_evidence_v1_point(target uuid, expected_current uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE elected uuid; previous_cutoff timestamptz; target_cutoff timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('bond_default_owner_evidence_v1',0));
    SELECT publication_id INTO elected FROM bond_default_owner_evidence_v1_pointer
        WHERE product='bond_default_owner_evidence_v1' FOR UPDATE;
    IF elected IS DISTINCT FROM expected_current THEN RAISE EXCEPTION 'owner evidence pointer CAS mismatch'; END IF;
    -- Zero adjudications is a valid receipt; positive validation is not a count floor.
    SELECT b.knowledge_cutoff INTO target_cutoff FROM bond_default_owner_evidence_v1_builds b
        JOIN bond_default_owner_evidence_v1_validations v USING (publication_id)
        WHERE b.publication_id=target AND v.bundle_sha256=b.bundle_sha256
            AND v.events_sha256=b.events_sha256 AND v.accepted_event_count=b.accepted_event_count
            AND v.receipt->>'verification'='replayed_export_and_stored_issue_rows';
    IF target_cutoff IS NULL THEN RAISE EXCEPTION 'owner evidence target is not validated'; END IF;
    SELECT knowledge_cutoff INTO previous_cutoff FROM bond_default_owner_evidence_v1_builds WHERE publication_id=elected;
    IF previous_cutoff IS NOT NULL AND target_cutoff<previous_cutoff THEN
        RAISE EXCEPTION 'owner evidence knowledge cutoff regression';
    END IF;
    INSERT INTO bond_default_owner_evidence_v1_pointer_tokens VALUES ('bond_default_owner_evidence_v1',pg_backend_pid());
    INSERT INTO bond_default_owner_evidence_v1_pointer(product,publication_id)
        VALUES ('bond_default_owner_evidence_v1',target)
        ON CONFLICT (product) DO UPDATE SET publication_id=EXCLUDED.publication_id,set_at=now();
    DELETE FROM bond_default_owner_evidence_v1_pointer_tokens WHERE product='bond_default_owner_evidence_v1';
END $$;

-- Bind invoker trigger/check functions as well as the definer CAS. A caller's
-- public/temporary search_path must never select another schema's evidence.
DO $$
DECLARE signature text; installed_schema text := current_schema();
BEGIN
    FOREACH signature IN ARRAY ARRAY[
        'bond_default_owner_evidence_v1_required_object(jsonb,text[])',
        'bond_default_owner_evidence_v1_immutable()',
        'bond_default_owner_evidence_v1_event_insert_guard()',
        'bond_default_owner_evidence_v1_validation_guard()',
        'bond_default_owner_evidence_v1_pointer_guard()',
        'bond_default_owner_evidence_v1_point(uuid,uuid)']
    LOOP
        EXECUTE format('ALTER FUNCTION %I.%s SET search_path TO pg_catalog, %I, pg_temp',
            installed_schema,signature,installed_schema);
    END LOOP;
END $$;

CREATE OR REPLACE VIEW bond_default_owner_evidence_v1_publications AS
SELECT b.publication_id, 'bond_default_owner_evidence_v1'::text AS product,
    b.policy_version,b.policy_digest,b.code_revision,b.code_digest,b.knowledge_cutoff,
    b.export_sha256,b.issuer_mapping_digest,b.events_sha256,b.bundle_sha256,
    b.proposal_count,b.decision_count,b.resolution_count,b.accepted_event_count,b.prepared_at,v.validated_at,
    CASE WHEN v.publication_id IS NULL THEN 'prepared' ELSE 'validated' END AS lifecycle_state,
    (p.publication_id=b.publication_id) IS TRUE AS is_current, false AS economic_authority
FROM bond_default_owner_evidence_v1_builds b LEFT JOIN bond_default_owner_evidence_v1_validations v USING (publication_id)
LEFT JOIN bond_default_owner_evidence_v1_pointer p USING (publication_id);
CREATE OR REPLACE VIEW bond_default_owner_events_v1_current AS
SELECT e.publication_id,e.event_id,e.decision_id,e.proposal_id,e.cusip9,e.event_date,e.event_type,
    e.link_sha256,e.payload,b.policy_digest,b.issuer_mapping_digest,false AS economic_authority
FROM bond_default_owner_evidence_v1_pointer p JOIN bond_default_owner_evidence_v1_validations v USING (publication_id)
JOIN bond_default_owner_evidence_v1_builds b USING (publication_id)
JOIN bond_default_owner_evidence_v1_events e USING (publication_id);

REVOKE ALL ON bond_default_owner_evidence_v1_builds,bond_default_owner_evidence_v1_events,
    bond_default_owner_evidence_v1_validations,bond_default_owner_evidence_v1_pointer,
    bond_default_owner_evidence_v1_pointer_tokens,bond_default_owner_evidence_v1_publications,
    bond_default_owner_events_v1_current FROM PUBLIC;
REVOKE ALL ON FUNCTION bond_default_owner_evidence_v1_point(uuid,uuid) FROM PUBLIC;
DO $$
DECLARE reader text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='worker_writer') THEN
        -- Reset old table grants on reapply, including the previous pointer/token bypass.
        REVOKE ALL ON bond_default_owner_evidence_v1_builds,bond_default_owner_evidence_v1_events,
            bond_default_owner_evidence_v1_validations,bond_default_owner_evidence_v1_pointer,
            bond_default_owner_evidence_v1_pointer_tokens,bond_default_owner_evidence_v1_publications,
            bond_default_owner_events_v1_current FROM worker_writer;
        GRANT SELECT,INSERT ON bond_default_owner_evidence_v1_builds,bond_default_owner_evidence_v1_events,
            bond_default_owner_evidence_v1_validations TO worker_writer;
        -- PostgreSQL FOR UPDATE requires UPDATE on any column. This one-column
        -- grant permits row locks, not rewrites: immutable triggers reject UPDATE.
        GRANT UPDATE(publication_id) ON bond_default_owner_evidence_v1_builds TO worker_writer;
        GRANT SELECT ON bond_default_owner_evidence_v1_pointer TO worker_writer;
        -- Token creation and pointer DML run only inside the owner-defined CAS.
        GRANT SELECT ON bond_default_owner_evidence_v1_publications,bond_default_owner_events_v1_current TO worker_writer;
        GRANT EXECUTE ON FUNCTION bond_default_owner_evidence_v1_point(uuid,uuid) TO worker_writer;
    END IF;
    FOREACH reader IN ARRAY ARRAY['app_runtime','app_analytics_ro'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname=reader) THEN
            EXECUTE format('REVOKE ALL ON bond_default_owner_evidence_v1_builds,bond_default_owner_evidence_v1_events,'
                'bond_default_owner_evidence_v1_validations,bond_default_owner_evidence_v1_pointer,'
                'bond_default_owner_evidence_v1_pointer_tokens,bond_default_owner_evidence_v1_publications,'
                'bond_default_owner_events_v1_current FROM %I',reader);
            EXECUTE format('REVOKE ALL ON FUNCTION bond_default_owner_evidence_v1_point(uuid,uuid) FROM %I',reader);
            EXECUTE format('GRANT SELECT ON bond_default_owner_evidence_v1_publications,'
                'bond_default_owner_events_v1_current TO %I',reader);
        END IF;
    END LOOP;
END $$;
