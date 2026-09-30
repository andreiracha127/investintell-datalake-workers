-- bond_rating_history_public_v1: full-grid public rating resolution (bundle frame
-- ratings). One row per (cusip_id, month, view_kind) of the pinned panel grid,
-- including excluded issues and the boundary start month; bucket is non-null only
-- for rights-approved observed / carried_verified actions. The N-PORT/EDGAR default
-- overlay references an episode and never claims an agency D.
--
-- Apply after bond_default_events_v1 with `SET search_path TO <schema>, pg_temp`.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DO $$
BEGIN
    IF pg_catalog.to_regclass('bond_default_event_v1') IS NULL THEN
        RAISE EXCEPTION 'bond_rating_history_public_v1: apply bond_default_events_v1 first (same schema)';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS bond_rating_history_public_v1 (
    publication_id uuid NOT NULL REFERENCES bond_credit_publications(publication_id),
    cusip_id char(9) NOT NULL CHECK (bond_credit_cusip9_valid(cusip_id)),
    month date NOT NULL CHECK (EXTRACT(day FROM month) = 1),
    view_kind text NOT NULL CHECK (view_kind IN ('effective_audit', 'public_pit')),
    bucket text CHECK (bucket IN ('A', 'AA', 'AAA', 'B', 'BB', 'BBB', 'CCC', 'D')),
    state text NOT NULL CHECK (state IN (
        'carried_verified', 'missing', 'observed', 'pit_unverified', 'rights_unverified', 'stale', 'withdrawn')),
    action_date date,
    public_known_at timestamptz,
    agency_source_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(agency_source_ids)),
    -- W0 amendment 1: persisted issue-scope links binding CUSIP-less relied actions to this row.
    binding_link_ids uuid[] NOT NULL CHECK (bond_credit_uuids_canonical(binding_link_ids)),
    coverage_frontier date,
    action_input_digest text CHECK (action_input_digest ~ '^sha256:[0-9a-f]{64}$'),
    default_overlay_episode_id uuid,
    row_sha256 char(64) NOT NULL CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (publication_id, cusip_id, month, view_kind),
    CHECK ((bucket IS NOT NULL) = (state IN ('observed', 'carried_verified'))),
    CHECK (state NOT IN ('observed', 'carried_verified')
           OR (action_date IS NOT NULL AND public_known_at IS NOT NULL
               AND cardinality(agency_source_ids) > 0 AND action_input_digest IS NOT NULL)),
    CHECK (cardinality(binding_link_ids) = 0 OR cardinality(agency_source_ids) > 0),
    CHECK (cardinality(agency_source_ids) > 0 OR action_input_digest IS NULL),
    CHECK (action_date IS NULL OR action_date < month + interval '1 month'),
    -- public_pit: proven public knowledge strictly before the next month boundary (UTC).
    CHECK (view_kind <> 'public_pit' OR state NOT IN ('observed', 'carried_verified')
           OR public_known_at < ((month + interval '1 month') AT TIME ZONE 'UTC'))
);
CREATE INDEX IF NOT EXISTS bond_rating_history_public_v1_month_idx
    ON bond_rating_history_public_v1 (publication_id, month, view_kind);

DROP TRIGGER IF EXISTS bond_rating_history_public_v1_insert_guard ON bond_rating_history_public_v1;
CREATE TRIGGER bond_rating_history_public_v1_insert_guard BEFORE INSERT ON bond_rating_history_public_v1
FOR EACH ROW EXECUTE FUNCTION bond_credit_child_insert_guard();
DROP TRIGGER IF EXISTS bond_rating_history_public_v1_append_only ON bond_rating_history_public_v1;
CREATE TRIGGER bond_rating_history_public_v1_append_only BEFORE UPDATE OR DELETE ON bond_rating_history_public_v1
FOR EACH ROW EXECUTE FUNCTION bond_credit_append_only();
DROP TRIGGER IF EXISTS bond_rating_history_public_v1_no_truncate ON bond_rating_history_public_v1;
CREATE TRIGGER bond_rating_history_public_v1_no_truncate BEFORE TRUNCATE ON bond_rating_history_public_v1
FOR EACH STATEMENT EXECUTE FUNCTION bond_credit_append_only();

-- Guarded output-frame reader (see bond_credit_read_publication in bond_credit_publications_v1.sql).
CREATE OR REPLACE FUNCTION bond_credit_read_ratings(target_publication_id uuid, allow_shadow boolean DEFAULT false)
RETURNS SETOF bond_rating_history_public_v1
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM bond_credit_read_publication(target_publication_id, allow_shadow);
    RETURN QUERY SELECT r.* FROM bond_rating_history_public_v1 r WHERE r.publication_id = target_publication_id;
END $$;

-- The serving reader reads only through the guarded bond_credit_read_ratings function.
REVOKE ALL ON bond_rating_history_public_v1 FROM PUBLIC, bond_credit_reader;
GRANT SELECT ON bond_rating_history_public_v1 TO bond_credit_auditor;
REVOKE ALL ON FUNCTION bond_credit_read_ratings(uuid, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bond_credit_read_ratings(uuid, boolean) TO bond_credit_reader;
GRANT SELECT, INSERT ON bond_rating_history_public_v1 TO bond_credit_writer;

COMMIT;
