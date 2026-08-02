-- Test fixture: the app-owned `fund_regulatory_serving_*` composition layer.
--
-- The composer lives here (workers repo) but the relations it writes are owned
-- by the app repo DDL:
--   backend/db/ddl/2026-07-18_sec_regulatory_serving_v2.sql
--   backend/db/ddl/2026-07-21_sec_regulatory_serving_families.sql
-- The app repo is not checked out in this repo's CI, so the load-bearing
-- subset is mirrored here VERBATIM: every table the composer writes, all three
-- write guards (publication immutability, prepared-only mappings, prepared-only
-- artifacts), the pointer guard, `fund_validate_regulatory_serving_publication_v2`,
-- `fund_set_current_regulatory_serving_publication_v2`, and the two serving
-- views whose behaviour the class_id fix is supposed to change.
--
-- DELIBERATELY OMITTED (nothing here writes or reads them): the legacy
-- `fund_regulatory_serving_current_pointers` mandate-only pointer, and the
-- `fund_regulatory_operating_profiles_v` / `fund_regulatory_fee_profiles_v`
-- views, whose snapshot tables (`ncen_operating_profiles`, `rr1_fee_profiles`)
-- are not installed by this fixture.
--
-- The real proof that the composer runs against the REAL DDL is the mirror run
-- recorded in .discovery/task-w2-report.md; this fixture proves the logic.

CREATE TABLE IF NOT EXISTS fund_regulatory_serving_publications (
    app_publication_id uuid PRIMARY KEY,
    app_publication_version integer NOT NULL UNIQUE
        CHECK (app_publication_version > 0),
    worker_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id),
    worker_publication_version integer NOT NULL CHECK (worker_publication_version > 0),
    lifecycle_state text NOT NULL DEFAULT 'prepared'
        CHECK (lifecycle_state IN ('prepared', 'validated')),
    prepared_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    CHECK ((lifecycle_state = 'prepared') = (validated_at IS NULL))
);

CREATE TABLE IF NOT EXISTS fund_regulatory_serving_artifacts (
    app_publication_id uuid NOT NULL
        REFERENCES fund_regulatory_serving_publications(app_publication_id) ON DELETE RESTRICT,
    product text NOT NULL CHECK (product IN (
        'regulatory_mandate', 'ncen_operating_profile_v1', 'rr1_fee_profile_v1',
        'sec_regulatory_serving_v1'
    )),
    worker_publication_id uuid NOT NULL REFERENCES sec_derived_publications(publication_id),
    worker_publication_version integer NOT NULL CHECK (worker_publication_version > 0),
    PRIMARY KEY (app_publication_id, product),
    UNIQUE (app_publication_id, worker_publication_id, worker_publication_version)
);

CREATE TABLE IF NOT EXISTS fund_regulatory_serving_app_current_pointer (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    app_publication_id uuid NOT NULL UNIQUE
        REFERENCES fund_regulatory_serving_publications(app_publication_id) ON DELETE RESTRICT,
    set_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS fund_regulatory_serving_app_pointer_tokens (
    singleton boolean PRIMARY KEY CHECK (singleton),
    backend_pid integer NOT NULL
);

CREATE TABLE IF NOT EXISTS fund_regulatory_serving_instrument_mappings (
    app_publication_id uuid NOT NULL
        REFERENCES fund_regulatory_serving_publications(app_publication_id) ON DELETE RESTRICT,
    instrument_id uuid NOT NULL,
    series_id text NOT NULL CHECK (btrim(series_id) <> ''),
    class_id text NOT NULL DEFAULT '',
    ncen_fund_id text,
    mapping_state text NOT NULL DEFAULT 'resolved'
        CHECK (mapping_state IN ('resolved', 'class_context_ambiguous')),
    PRIMARY KEY (app_publication_id, instrument_id)
);

CREATE INDEX IF NOT EXISTS fund_regulatory_serving_mapping_lookup_idx
    ON fund_regulatory_serving_instrument_mappings
    (app_publication_id, series_id, class_id, instrument_id);

CREATE TABLE IF NOT EXISTS fund_regulatory_serving_validation_tokens (
    app_publication_id uuid PRIMARY KEY,
    backend_pid integer NOT NULL
);

CREATE OR REPLACE FUNCTION fund_regulatory_serving_publication_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT'
       AND NEW.lifecycle_state = 'prepared'
       AND NEW.validated_at IS NULL THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE'
       AND OLD.lifecycle_state = 'prepared'
       AND NEW.lifecycle_state = 'validated'
       AND NEW.validated_at IS NOT NULL
       AND NEW.app_publication_id = OLD.app_publication_id
       AND NEW.worker_publication_id = OLD.worker_publication_id
       AND NEW.worker_publication_version = OLD.worker_publication_version
       AND NEW.prepared_at = OLD.prepared_at
       AND EXISTS (
           SELECT 1 FROM fund_regulatory_serving_validation_tokens token
           WHERE token.app_publication_id = OLD.app_publication_id
             AND token.backend_pid = pg_backend_pid()
       ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'regulatory serving publication is immutable';
END $$;

DROP TRIGGER IF EXISTS fund_regulatory_serving_publication_immutable
    ON fund_regulatory_serving_publications;
CREATE TRIGGER fund_regulatory_serving_publication_immutable
BEFORE INSERT OR UPDATE OR DELETE ON fund_regulatory_serving_publications
FOR EACH ROW EXECUTE FUNCTION fund_regulatory_serving_publication_guard();

CREATE OR REPLACE FUNCTION fund_regulatory_serving_mapping_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_publication_id uuid := CASE WHEN TG_OP = 'DELETE'
        THEN OLD.app_publication_id ELSE NEW.app_publication_id END;
    parent_state text;
BEGIN
    SELECT lifecycle_state INTO parent_state
    FROM fund_regulatory_serving_publications
    WHERE app_publication_id = target_publication_id
    FOR UPDATE;
    IF parent_state = 'prepared' THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    RAISE EXCEPTION 'regulatory serving mappings require a prepared app publication';
END $$;

DROP TRIGGER IF EXISTS fund_regulatory_serving_mapping_immutable
    ON fund_regulatory_serving_instrument_mappings;
CREATE TRIGGER fund_regulatory_serving_mapping_immutable
BEFORE INSERT OR UPDATE OR DELETE ON fund_regulatory_serving_instrument_mappings
FOR EACH ROW EXECUTE FUNCTION fund_regulatory_serving_mapping_guard();

CREATE OR REPLACE FUNCTION fund_regulatory_serving_artifact_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target uuid := CASE WHEN TG_OP = 'DELETE' THEN OLD.app_publication_id ELSE NEW.app_publication_id END;
        parent_state text;
BEGIN
    SELECT lifecycle_state INTO parent_state
      FROM fund_regulatory_serving_publications
     WHERE app_publication_id = target
     FOR UPDATE;
    IF parent_state = 'prepared' THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;
    RAISE EXCEPTION 'serving artifacts require a prepared app publication';
END $$;
DROP TRIGGER IF EXISTS fund_regulatory_serving_artifact_immutable ON fund_regulatory_serving_artifacts;
CREATE TRIGGER fund_regulatory_serving_artifact_immutable
BEFORE INSERT OR UPDATE OR DELETE ON fund_regulatory_serving_artifacts
FOR EACH ROW EXECUTE FUNCTION fund_regulatory_serving_artifact_guard();

CREATE OR REPLACE FUNCTION fund_regulatory_serving_app_pointer_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM fund_regulatory_serving_app_pointer_tokens
               WHERE singleton=true AND backend_pid=pg_backend_pid()) THEN
        RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
    END IF;
    RAISE EXCEPTION 'serving app pointer is managed by its switch function';
END $$;
DROP TRIGGER IF EXISTS fund_regulatory_serving_app_pointer_managed ON fund_regulatory_serving_app_current_pointer;
CREATE TRIGGER fund_regulatory_serving_app_pointer_managed
BEFORE INSERT OR UPDATE OR DELETE ON fund_regulatory_serving_app_current_pointer
FOR EACH ROW EXECUTE FUNCTION fund_regulatory_serving_app_pointer_guard();

CREATE OR REPLACE FUNCTION fund_validate_regulatory_serving_publication_v2(target_app_publication_id uuid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE serving fund_regulatory_serving_publications%ROWTYPE;
BEGIN
    SELECT * INTO serving FROM fund_regulatory_serving_publications
     WHERE app_publication_id = target_app_publication_id FOR UPDATE;
    IF serving.app_publication_id IS NULL OR serving.lifecycle_state <> 'prepared' THEN
        RAISE EXCEPTION 'validation requires a prepared app publication';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM fund_regulatory_serving_artifacts a
                   JOIN sec_derived_publications w ON w.publication_id = a.worker_publication_id
                  WHERE a.app_publication_id = target_app_publication_id
                    AND a.product = 'regulatory_mandate'
                    AND w.product = a.product AND w.publication_version = a.worker_publication_version
                    AND w.lifecycle_state = 'validated') THEN
        RAISE EXCEPTION 'app publication requires an exact validated mandate artifact';
    END IF;
    IF EXISTS (SELECT 1 FROM fund_regulatory_serving_artifacts a
               LEFT JOIN sec_derived_publications w ON w.publication_id = a.worker_publication_id
              WHERE a.app_publication_id = target_app_publication_id
                AND (w.product IS DISTINCT FROM a.product
                     OR w.publication_version IS DISTINCT FROM a.worker_publication_version
                     OR w.lifecycle_state IS DISTINCT FROM 'validated')) THEN
        RAISE EXCEPTION 'app artifact must pin its exact validated worker product and version';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM fund_regulatory_serving_instrument_mappings
                   WHERE app_publication_id = target_app_publication_id) THEN
        RAISE EXCEPTION 'app publication requires instrument mappings';
    END IF;
    INSERT INTO fund_regulatory_serving_validation_tokens(app_publication_id, backend_pid)
    VALUES (target_app_publication_id, pg_backend_pid()) ON CONFLICT DO NOTHING;
    UPDATE fund_regulatory_serving_publications SET lifecycle_state='validated', validated_at=now()
     WHERE app_publication_id = target_app_publication_id;
    DELETE FROM fund_regulatory_serving_validation_tokens
     WHERE app_publication_id=target_app_publication_id AND backend_pid=pg_backend_pid();
END $$;

CREATE OR REPLACE FUNCTION fund_set_current_regulatory_serving_publication_v2(target_app_publication_id uuid)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('fund_regulatory_serving_app_current_pointer', 0));
    IF NOT EXISTS (SELECT 1 FROM fund_regulatory_serving_publications
                   WHERE app_publication_id=target_app_publication_id AND lifecycle_state='validated') THEN
        RAISE EXCEPTION 'current app pointer requires a validated app publication';
    END IF;
    INSERT INTO fund_regulatory_serving_app_pointer_tokens(singleton, backend_pid)
    VALUES (true, pg_backend_pid())
    ON CONFLICT (singleton) DO UPDATE SET backend_pid=EXCLUDED.backend_pid;
    INSERT INTO fund_regulatory_serving_app_current_pointer(singleton, app_publication_id)
    VALUES (true, target_app_publication_id)
    ON CONFLICT (singleton) DO UPDATE SET app_publication_id=EXCLUDED.app_publication_id, set_at=now();
    DELETE FROM fund_regulatory_serving_app_pointer_tokens WHERE singleton=true AND backend_pid=pg_backend_pid();
END $$;

CREATE OR REPLACE VIEW fund_regulatory_mandate_profiles_v AS
WITH current_app AS (
  SELECT p.app_publication_id, s.app_publication_version
  FROM fund_regulatory_serving_app_current_pointer p
  JOIN fund_regulatory_serving_publications s ON s.app_publication_id=p.app_publication_id
   AND s.lifecycle_state='validated'
), mandate_artifact AS (
  SELECT a.* FROM current_app c JOIN fund_regulatory_serving_artifacts a ON a.app_publication_id=c.app_publication_id
  WHERE a.product='regulatory_mandate'
), candidate_pool AS (
  SELECT c.app_publication_id,m.instrument_id, CASE WHEN x.class_id=m.class_id THEN 0 ELSE 1 END priority, x.*
  FROM current_app c JOIN fund_regulatory_serving_instrument_mappings m ON m.app_publication_id=c.app_publication_id
  JOIN mandate_artifact a ON a.app_publication_id=c.app_publication_id
  JOIN sec_regulatory_mandate_profiles x ON x.publication_id=a.worker_publication_id AND x.series_id=m.series_id AND x.class_id IN (m.class_id,'')
  WHERE m.mapping_state='resolved'
), ranked AS (
  SELECT *, min(priority) OVER (PARTITION BY app_publication_id,instrument_id) best_priority FROM candidate_pool
), selected AS (
  SELECT *, count(*) OVER (PARTITION BY app_publication_id,instrument_id) candidate_count,
         row_number() OVER (PARTITION BY app_publication_id,instrument_id ORDER BY document_id) candidate_number
  FROM ranked WHERE priority=best_priority
)
SELECT m.instrument_id, c.app_publication_version AS publication_version,
  CASE WHEN m.mapping_state='class_context_ambiguous' OR s.candidate_count>1 THEN 'class_context_ambiguous'
       WHEN s.publication_id IS NULL THEN 'source_filing_unavailable' ELSE 'published' END selection_state,
  s.objective_text,s.objective_state,s.objective_reason_code,s.objective_source_date,s.objective_provenance,
  s.strategy_text,s.strategy_state,s.strategy_reason_code,s.strategy_source_date,s.strategy_provenance,
  s.concentration_policy_text,s.concentration_policy_state,s.concentration_policy_reason_code,s.concentration_policy_source_date,s.concentration_policy_provenance,
  s.principal_risks_text,s.principal_risks_state,s.principal_risks_reason_code,s.principal_risks_source_date,s.principal_risks_provenance
FROM current_app c JOIN fund_regulatory_serving_instrument_mappings m ON m.app_publication_id=c.app_publication_id
LEFT JOIN selected s ON s.app_publication_id=m.app_publication_id AND s.instrument_id=m.instrument_id AND s.candidate_number=1;

CREATE OR REPLACE VIEW fund_regulatory_serving_facts_v AS
SELECT
    m.instrument_id,
    s.app_publication_version AS publication_version,
    a.worker_publication_version,
    CASE
        WHEN m.mapping_state = 'class_context_ambiguous' THEN 'class_context_ambiguous'
        WHEN f.publication_id IS NULL THEN 'source_filing_unavailable'
        ELSE 'published'
    END AS selection_state,
    f.family,
    f.series_id,
    f.class_id,
    f.fund_id,
    f.fact_key,
    f.grain_origin,
    f.state,
    f.reason_code,
    f.snapshot_reason_code,
    f.coverage_pct,
    f.source_date,
    f.accession_number,
    f.document_id,
    f.filing_date,
    f.effective_date,
    f.payload
FROM fund_regulatory_serving_app_current_pointer p
JOIN fund_regulatory_serving_publications s
    ON s.app_publication_id = p.app_publication_id AND s.lifecycle_state = 'validated'
JOIN fund_regulatory_serving_instrument_mappings m
    ON m.app_publication_id = s.app_publication_id
LEFT JOIN fund_regulatory_serving_artifacts a
    ON a.app_publication_id = s.app_publication_id AND a.product = 'sec_regulatory_serving_v1'
LEFT JOIN sec_regulatory_serving_facts f
    ON f.publication_id = a.worker_publication_id
   AND (
        (f.grain_origin IN ('fund', 'registrant')
            AND f.fund_id = m.ncen_fund_id
            AND (f.series_id = m.series_id OR f.series_id = ''))
        OR (f.grain_origin = 'class'
            AND f.series_id = m.series_id
            AND f.class_id IN (m.class_id, ''))
        OR (f.grain_origin = 'series' AND f.series_id = m.series_id)
   );
