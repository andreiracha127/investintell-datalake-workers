-- Immutable, fixture-safe publication metadata.  It never reads raw row tables.
CREATE TABLE IF NOT EXISTS sec_derived_publications (
    publication_id uuid PRIMARY KEY,
    product text NOT NULL CHECK (product <> ''),
    publication_version integer NOT NULL CHECK (publication_version > 0),
    source_run_id uuid NOT NULL REFERENCES sec_ingestion_runs(run_id) ON DELETE RESTRICT,
    source_package_id uuid NOT NULL REFERENCES sec_source_packages(package_id) ON DELETE RESTRICT,
    build_fingerprint char(64) NOT NULL CHECK (build_fingerprint ~ '^[0-9a-f]{64}$'),
    prepared_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    lifecycle_state text NOT NULL DEFAULT 'prepared' CHECK (lifecycle_state IN ('prepared', 'validated')),
    UNIQUE(product, publication_version),
    CHECK ((lifecycle_state='prepared') = (validated_at IS NULL))
);

CREATE TABLE IF NOT EXISTS sec_derived_current_pointers (
    product text PRIMARY KEY CHECK (product <> ''),
    publication_id uuid NOT NULL UNIQUE REFERENCES sec_derived_publications(publication_id) ON DELETE RESTRICT,
    set_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sec_derived_publication_tokens (
    publication_id uuid PRIMARY KEY REFERENCES sec_derived_publications(publication_id) ON DELETE CASCADE,
    backend_pid integer NOT NULL
);
CREATE TABLE IF NOT EXISTS sec_derived_pointer_tokens (
    product text PRIMARY KEY,
    backend_pid integer NOT NULL
);
REVOKE ALL ON sec_derived_publication_tokens, sec_derived_pointer_tokens FROM PUBLIC;

CREATE OR REPLACE FUNCTION sec_derived_publication_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND OLD.lifecycle_state='prepared' AND NEW.lifecycle_state='validated'
       AND NEW.validated_at IS NOT NULL AND NEW.publication_id=OLD.publication_id
       AND NEW.product=OLD.product AND NEW.publication_version=OLD.publication_version
       AND NEW.source_run_id=OLD.source_run_id AND NEW.source_package_id=OLD.source_package_id
       AND NEW.build_fingerprint=OLD.build_fingerprint AND NEW.prepared_at=OLD.prepared_at
       AND EXISTS (SELECT 1 FROM sec_derived_publication_tokens t
                   WHERE t.publication_id=OLD.publication_id AND t.backend_pid=pg_backend_pid()) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'derived publication is immutable';
END $$;

CREATE OR REPLACE FUNCTION sec_derived_publication_delete_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.lifecycle_state='validated' THEN
        RAISE EXCEPTION 'validated derived publication cannot be deleted';
    END IF;
    IF EXISTS (SELECT 1 FROM sec_derived_current_pointers p WHERE p.publication_id=OLD.publication_id) THEN
        RAISE EXCEPTION 'current derived publication cannot be deleted';
    END IF;
    RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS sec_derived_publications_immutable ON sec_derived_publications;
CREATE TRIGGER sec_derived_publications_immutable BEFORE UPDATE ON sec_derived_publications
FOR EACH ROW EXECUTE FUNCTION sec_derived_publication_immutable();
DROP TRIGGER IF EXISTS sec_derived_publications_delete_guard ON sec_derived_publications;
CREATE TRIGGER sec_derived_publications_delete_guard BEFORE DELETE ON sec_derived_publications
FOR EACH ROW EXECUTE FUNCTION sec_derived_publication_delete_guard();

CREATE OR REPLACE FUNCTION sec_derived_pointer_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE pointer_product text := COALESCE(NEW.product, OLD.product);
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sec_derived_pointer_tokens t
                   WHERE t.product=pointer_product AND t.backend_pid=pg_backend_pid()) THEN
        RAISE EXCEPTION 'current pointer is managed by sec_set_current_derived_publication';
    END IF;
    RETURN COALESCE(NEW, OLD);
END $$;
DROP TRIGGER IF EXISTS sec_derived_current_pointer_guard ON sec_derived_current_pointers;
CREATE TRIGGER sec_derived_current_pointer_guard BEFORE INSERT OR UPDATE OR DELETE ON sec_derived_current_pointers
FOR EACH ROW EXECUTE FUNCTION sec_derived_pointer_guard();

CREATE OR REPLACE FUNCTION sec_validate_derived_publication(target_publication_id uuid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE publication sec_derived_publications%ROWTYPE;
BEGIN
    SELECT * INTO publication FROM sec_derived_publications WHERE publication_id=target_publication_id FOR UPDATE;
    IF publication.publication_id IS NULL THEN RAISE EXCEPTION 'derived publication does not exist'; END IF;
    IF publication.lifecycle_state<>'prepared' THEN RAISE EXCEPTION 'derived publication is already validated'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM sec_validated_raw_runs r
        JOIN sec_ingestion_runs ir ON ir.run_id=r.run_id AND ir.raw_validated_at IS NOT NULL
        JOIN sec_source_packages p ON p.package_id=publication.source_package_id AND p.run_id=r.run_id
        WHERE r.run_id=publication.source_run_id
    ) THEN RAISE EXCEPTION 'derived publication requires a validated source run and package lineage'; END IF;
    INSERT INTO sec_derived_publication_tokens(publication_id,backend_pid) VALUES(target_publication_id,pg_backend_pid());
    UPDATE sec_derived_publications SET lifecycle_state='validated', validated_at=now() WHERE publication_id=target_publication_id;
    DELETE FROM sec_derived_publication_tokens WHERE publication_id=target_publication_id AND backend_pid=pg_backend_pid();
END $$;

CREATE OR REPLACE FUNCTION sec_derived_publication_is_validated(target_publication_id uuid, expected_product text DEFAULT NULL)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1 FROM sec_derived_publications p
        WHERE p.publication_id=target_publication_id AND p.lifecycle_state='validated'
          AND (expected_product IS NULL OR p.product=expected_product)
    )
$$;

-- The data date ("as_of") a publication was built for.  Every derived builder
-- records it on its own `<product>_builds` row (publication_id PRIMARY KEY,
-- as_of_date) at build time, i.e. before the publication can ever be promoted;
-- no publication_id appears in two build tables, so the lookup is unambiguous.
-- Returns NULL when the product keeps no build row (no data date to order by).
CREATE OR REPLACE FUNCTION sec_derived_publication_as_of(target_publication_id uuid)
RETURNS date LANGUAGE plpgsql STABLE AS $$
DECLARE build_table regclass; found date;
BEGIN
    IF target_publication_id IS NULL THEN RETURN NULL; END IF;
    FOR build_table IN
        SELECT c.oid::regclass FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind='r' AND pg_table_is_visible(c.oid)
          AND n.nspname NOT IN ('pg_catalog','information_schema')
          AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid
                      AND a.attname='publication_id' AND a.attnum>0 AND NOT a.attisdropped)
          AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid
                      AND a.attname='as_of_date' AND a.attnum>0 AND NOT a.attisdropped)
        ORDER BY c.relname
    LOOP
        EXECUTE format('SELECT max(as_of_date) FROM %s WHERE publication_id = $1', build_table)
            INTO found USING target_publication_id;
        IF found IS NOT NULL THEN RETURN found; END IF;
    END LOOP;
    RETURN NULL;
END $$;

-- Monotonic promotion.  The 3-argument form replaces the 2-argument one (a
-- defaulted third parameter would make 2-arg calls ambiguous), so existing
-- 2-argument call sites keep working and get the guard by default.
DROP FUNCTION IF EXISTS sec_set_current_derived_publication(text, uuid);
CREATE OR REPLACE FUNCTION sec_set_current_derived_publication(
    target_product text,
    target_publication_id uuid,
    allow_as_of_regression boolean DEFAULT false
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    publication sec_derived_publications%ROWTYPE;
    current_publication_id uuid;
    current_as_of date;
    target_as_of date;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(target_product, 0));
    SELECT * INTO publication FROM sec_derived_publications WHERE publication_id=target_publication_id FOR UPDATE;
    IF publication.publication_id IS NULL OR publication.product<>target_product OR publication.lifecycle_state<>'validated' THEN
        RAISE EXCEPTION 'current pointer requires a validated publication for its product';
    END IF;
    -- The current pointer never moves BACKWARD in data date: a catch-up run over
    -- historical source-days must not make an older (or empty) build current.  A
    -- deliberate rollback/repoint asks for it explicitly.
    SELECT publication_id INTO current_publication_id
    FROM sec_derived_current_pointers WHERE product=target_product;
    IF NOT allow_as_of_regression AND current_publication_id IS NOT NULL
       AND current_publication_id <> target_publication_id THEN
        current_as_of := sec_derived_publication_as_of(current_publication_id);
        target_as_of := sec_derived_publication_as_of(target_publication_id);
        IF current_as_of IS NOT NULL AND target_as_of IS NOT NULL AND target_as_of < current_as_of THEN
            RAISE EXCEPTION
                'current pointer for % would regress from as_of % to as_of %',
                target_product, current_as_of, target_as_of
            USING HINT = 'deliberate rollback: sec_set_current_derived_publication('
                         '<product>, <publication_id>, allow_as_of_regression => true)';
        END IF;
    END IF;
    INSERT INTO sec_derived_pointer_tokens(product,backend_pid) VALUES(target_product,pg_backend_pid())
    ON CONFLICT (product) DO UPDATE SET backend_pid=EXCLUDED.backend_pid;
    INSERT INTO sec_derived_current_pointers(product,publication_id) VALUES(target_product,target_publication_id)
    ON CONFLICT (product) DO UPDATE SET publication_id=EXCLUDED.publication_id, set_at=now();
    DELETE FROM sec_derived_pointer_tokens WHERE product=target_product AND backend_pid=pg_backend_pid();
END $$;

CREATE OR REPLACE VIEW sec_current_derived_publications AS
SELECT p.* FROM sec_derived_current_pointers c JOIN sec_derived_publications p ON p.publication_id=c.publication_id;
